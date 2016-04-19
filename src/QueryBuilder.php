<?php
namespace Vda\Datasource\DocumentOriented\Elastic;

use Elasticsearch\Common\Exceptions\InvalidArgumentException;
use Vda\Query\Alias;
use Vda\Query\Delete;
use Vda\Query\Field;
use Vda\Query\Insert;
use Vda\Query\IQueryPart;
use Vda\Query\IQueryProcessor;
use Vda\Query\JoinClause;
use Vda\Query\Operator\BinaryOperator;
use Vda\Query\Operator\CompositeOperator;
use Vda\Query\Operator\Constant;
use Vda\Query\Operator\FunctionCall;
use Vda\Query\Operator\Mask;
use Vda\Query\Operator\Operator;
use Vda\Query\Operator\UnaryOperator;
use Vda\Query\Order;
use Vda\Query\Select;
use Vda\Query\Table;
use Vda\Query\Update;
use Vda\Query\Upsert;
use Vda\Util\Type;

class QueryBuilder implements IQueryProcessor
{
    private static $aggregateFunctions = ['count', 'sum', 'avg', 'min', 'max'];

    private $result;

    public function build(IQueryPart $processable)
    {
        $this->result = null;

        $processable->onProcess($this);

        return $this->result;
    }

    public function processSelectQuery(Select $query)
    {
        if (!empty($this->result)) {
           throw new \Exception('Nested select queries are not supported yet');
        }

        $sources = $query->getSources();

        if (count($sources) > 1) {
            throw new \Exception('Multiple sources are not supported yet');
        }

        $s = reset($sources)->onProcess($this);

        if ($s['class'] != 'table') {
            throw new \Exception("Unsupported source type: {$s['class']}");
        }

        $this->result['index'] = $s['schema'];
        $this->result['type'] = $s['name'];

        //TODO Handle joins and nested select clauses
        $fields = $query->getFields();
        $aggFuncs = [];

        foreach ($fields as $field) {
            $f = $field->onProcess($this);

            if ($f['class'] == 'field') {
                $this->result['body']['_source'][] = $f['name'];
                $this->result['qb']['has-fields'] = true;
            } elseif ($f['class'] == 'func') {
                if (in_array($f['name'], self::$aggregateFunctions)) {
                    $f['agg-id'] = uniqid();
                    $aggFuncs[] = $f;
                } else {
                    //TODO Add support for non-aggregate functions
                    throw new \Exception("Unsupported function call: {$f['name']}");
                }
            } else {
                throw new \Exception("Unsupported field class: {$f['class']}");
            }

            $this->result['qb']['fields'][] = $f;
        }

        $this->buildCriteria($query->getCriteria());

        $this->buildAggregations($query->getGroups(), $aggFuncs);

        $this->buildOrders($query->getOrders());

        $this->buildPagination($query->getLimit(), $query->getOffset());

        //TODO Check if we may do something about this
        //$this->buildLockMode($select->getLockMode());

        return $this->result;
    }

    public function processInsertQuery(Insert $query)
    {
        if ($query->isFromSelect()) {
            throw new \Exception("INSERT ... SELECT is not supported yet");
        }

        $t = $query->getTable()->onProcess($this);

        $this->result['index'] = $t['schema'];
        $this->result['type'] = $t['name'];

        $fields = $query->hasFields() ? $query->getFields() : $t['fields'];

        $emptyObj = new \stdClass();

        foreach ($query->getValues() as $values) {
            $insert = $this->buildDoc($fields, $values);

            if (empty($insert['id'])) {
                $this->result['body'][] = ['create' => $emptyObj];
            } else {
                $this->result['body'][] = ['create' => ['_id' => $insert['id']]];
            }
            $this->result['body'][] = $insert['doc'];
        }

        return $this->result;
    }

    public function processUpdateQuery(Update $query)
    {
        $tables = $query->getTables();

        if (count($tables) > 1) {
            throw new \Exception('Updating multiple indices is not supported yet');
        }

        $t = reset($tables)->onProcess($this);

        $this->result['index'] = $t['schema'];
        $this->result['type'] = $t['name'];

        $update = $this->buildDoc($query->getFields(), $query->getExpressions());

        $criteria = $query->getCriteria();

        if (!empty($criteria)) {
            $c = $criteria->onProcess($this)['term'];
            if (count($c) != 1 || empty($c['ids']) || count($c['ids']['values']) != 1) {
                throw new \InvalidArgumentException(
                    'Only updating a single entry denoted by id is currently supported'
                );
            }
            $update['id'] = reset($c['ids']['values']);
        } elseif (empty($update['id'])) {
            throw new \Exception('Only update by id is currently supported, none given');
        }

        $this->result['body'][] = ['update' => ['_id' => $update['id']]];
        $this->result['body'][] = ['doc' => $update['doc']];

        return $this->result;
    }

    public function processUpsertQuery(Upsert $query)
    {
        $t = $query->getTable()->onProcess($this);

        $this->result['index'] = $t['schema'];
        $this->result['type'] = $t['name'];

        $insert = $this->buildDoc($query->getInsertFields(), $query->getInsertValues());

        if (empty($insert['id'])) {
            $this->result['body'][] = ['create' => new \stdClass()];
            $this->result['body'][] = $insert['doc'];

            return $this->result;
        }

        $update = $this->buildDoc($query->getUpdateFields(), $query->getUpdateValues());

        $this->result['body'][] = ['update' => ['_id' => $insert['id']]];
        $this->result['body'][] = ['doc' => $update['doc'], 'upsert' => $insert['doc']];

        return $this->result;
    }

    public function processDeleteQuery(Delete $query)
    {
        $tables = $query->getTables();

        if (count($tables) > 1) {
            throw new \Exception('Deletion from multiple indices is not supported yet');
        }

        $t = reset($tables)->onProcess($this);

        $this->result['index'] = $t['schema'];
        $this->result['type'] = $t['name'];

        $this->buildCriteria($query->getCriteria());

        return $this->result;
    }

    public function processField(Field $field)
    {
            return ['name' => $field->getName(), 'class' => 'field', 'type' => $field->getType()];
    }

    public function processTable(Table $table)
    {
        return [
            'class'  => 'table',
            'schema' => $table->getSchema(),
            'name'   => $table->getName(),
            'fields' => $table->getFields(),
        ];
    }

    public function processJoin(JoinClause $join)
    {
        throw new \Exception('Join clause is not supported yet');
    }

    public function processUnaryOperator(UnaryOperator $operator)
    {
        $operand = $operator->getOperand()->onProcess($this);

        switch ($operator->getMnemonic()) {
            case Operator::MNEMONIC_UNARY_NOT:
                if ($operand['class'] == 'filter') {
                    $result['term'] = ['not' => $operand['term']];
                } else {
                    $result['term'] = ['bool' => ['must_not' => $operand['term']]];
                }
                break;

            case Operator::MNEMONIC_UNARY_ISNULL:
                $result = [
                    'class' => 'filter',
                    'term' => ['missing' => ['field' => $operand['name']]],
                ];
                break;

            case Operator::MNEMONIC_UNARY_NOTNULL:
                $result = [
                    'class' => 'filter',
                    'term' => ['exists' => ['field' => $operand['name']]],
                ];
                break;

            default:
                $this->onInvalidMnemonic('unary', $op->getMnemonic());
        }

        return $result;
    }

    public function processBinaryOperator(BinaryOperator $operator)
    {
        $op = $operator->getMnemonic();
        $rangeOperators = [
            Operator::MNEMONIC_BINARY_GT  => 'gt',
            Operator::MNEMONIC_BINARY_LT  => 'lt',
            Operator::MNEMONIC_BINARY_GTE => 'gte',
            Operator::MNEMONIC_BINARY_LTE => 'lte',
        ];

        $negate = in_array(
            $op,
            [
                Operator::MNEMONIC_BINARY_NEQ,
                Operator::MNEMONIC_BINARY_NOTINSET,
                Operator::MNEMONIC_BINARY_NOTMATCH,
                Operator::MNEMONIC_BINARY_NOTMATCHI,
            ]
        );

        $field = $operator->getOperand1()->onProcess($this);
        $expression = $operator->getOperand2()->onProcess($this);

        if ($field['class'] != 'field') {
            throw new \InvalidArgumentException('Only sorting by fields is supported currently');
        }

        switch ($op) {
            case Operator::MNEMONIC_BINARY_GT:
            case Operator::MNEMONIC_BINARY_LT:
            case Operator::MNEMONIC_BINARY_GTE:
            case Operator::MNEMONIC_BINARY_LTE:

                $result = [
                    'class' => 'filter',
                    'term' => ['range' => [$field['name'] => [$rangeOperators[$op] => $expression['term']]]],
                ];
                break;

            case Operator::MNEMONIC_BINARY_INSET:
            case Operator::MNEMONIC_BINARY_NOTINSET:
                if ($field['name'] == 'id') {
                    $result = [
                        'class' => 'filter',
                        'term' => ['ids' => ['values' => array_values($expression['term'])]],
                    ];
                } else {
                    $result = [
                        'class' => 'filter',
                        'term' => ['terms' => [$field['name'] => array_values($expression['term'])]],
                    ];
                }
                break;

            case Operator::MNEMONIC_BINARY_EQ:
            case Operator::MNEMONIC_BINARY_NEQ:
                if ($field['name'] == 'id') {
                    $result = [
                        'class' => 'filter',
                        'term' => ['ids' => ['values' => (array)$expression['term']]],
                    ];
                } else {
                    $result = [
                        'class' => 'filter',
                        'term' => ['term' => [$field['name'] => $expression['term']]],
                    ];
                }
                break;

            case Operator::MNEMONIC_BINARY_MATCH:
            case Operator::MNEMONIC_BINARY_MATCHI:
            case Operator::MNEMONIC_BINARY_NOTMATCH:
            case Operator::MNEMONIC_BINARY_NOTMATCHI:
                $m = empty($expression['wildcard']) ? 'match' : 'wildcard';

                if (is_array($expression['term'])) {
                    $result = ['class' => 'query', 'term' => []];

                    foreach ($expression['term'] as $term) {
                        $result['term']['bool']['should'][] = [$m => [$field['name'] => $term]];
                    }
                } else {
                    $result = [
                        'class' => 'query',
                        'term' => [$m => [$field['name'] => $expression['term']]],
                    ];
                }
                break;

            case Operator::MNEMONIC_BINARY_MINUS:
            case Operator::MNEMONIC_BINARY_DIVIDE:
                //TODO
                break;

            default:
                $this->onInvalidMnemonic('binary', $op);
        }

        if ($negate) {
            $result = $this->negateQuery($result);
        }

        return $result;
    }

    public function processCompositeOperator(CompositeOperator $operator)
    {
        $filters = [];
        $queries = [];
        $result  = [];

        foreach ($operator->getOperands() as $operand) {
            $o = $operand->onProcess($this);
            if ($o['class'] == 'filter') {
                $filters[] = $o['term'];
            } elseif ($o['class'] == 'query') {
                $queries[] = $o['term'];
            } else {
                //TODO Handle operands for '+' and '*'
            }
        }

        switch ($operator->getMnemonic()) {
            case Operator::MNEMONIC_COMPOSITE_AND:
                if (!empty($filters) && !empty($queries)) {
                    $result = [
                        'class' => 'query',
                        'term' => [
                            'filtered' => [
                                'query'  => ['bool' => ['must' => $queries]],
                                'filter' => ['and' => $filters],
                            ],
                        ],
                    ];
                } elseif (!empty($queries)) {
                    $result = [
                        'class' => 'query',
                        'term' => [
                            'bool' => ['must' => $queries],
                        ]
                    ];
                } elseif (!empty($filters)) {
                    $result = [
                        'class' => 'filter',
                        'term' => ['and' => $filters],
                    ];
                }
                break;

            case Operator::MNEMONIC_COMPOSITE_OR:
                if (!empty($filters) && !empty($queries)) {
                    $result = [
                        'class' => 'query',
                        'term' => [
                            'filtered' => [
                                'query'  => ['bool' => ['should' => $queries]],
                                'filter' => ['or' => $filters],
                            ],
                        ],
                    ];
                } elseif (!empty($queries)) {
                    $result = [
                        'class' => 'query',
                        'term' => [
                            'bool' => ['should' => $queries],
                        ]
                    ];
                } elseif (!empty($filters)) {
                    $result = [
                        'class' => 'filter',
                        'term' => ['or' => $filters],
                    ];
                }
                break;

            case Operator::MNEMONIC_COMPOSITE_PLUS:
                //TODO Implement this
                break;

            case Operator::MNEMONIC_COMPOSITE_MULTIPLY:
                //TODO Implement this
                break;

            default:
                $this->onInvalidMnemonic('composite', $operator->getMnemonic());
        }

        return $result;
    }

    public function processConstant(Constant $const)
    {
        $term = $const->getValue();

        if ($const->getType() == Type::DATE) {
            $timestamp = false;
            if ($term instanceof \DateTimeInterface) {
                $timestamp = $term->getTimestamp();
            } elseif (is_array($term) && !empty($term['timestamp'])) {
                $timestamp = $term['timestamp'];
            } elseif (is_string($term)) {
                $timestamp = strtotime($term);
            }

            if ($timestamp === false) {
                throw new \InvalidArgumentException(
                    "The '{$term}' literal must be a valid date"
                );
            }

            $term = date(\DateTime::ISO8601, $timestamp);
        }

        return ['class' => 'constant', 'term' => $term];
    }

    public function processMask(Mask $mask)
    {
        if ($mask->getMnemonic() != Operator::MNEMONIC_CONST) {
            $this->onInvalidMnemonic('mask', $mask->getMnemonic());
        }

        $mask = $mask->getMask();

        $numEscapedAsterisks = substr_count($mask, '\\*');
        $numEscapedQuestions = substr_count($mask, '\\?');

        if (
            substr_count($mask, '*') > $numEscapedAsterisks ||
            substr_count($mask, '?') > $numEscapedQuestions
        ) {
            $result = [
                'wildcard' => true,
                'term' => $mask,
            ];
        } else {
            $result = [
                'wildcard' => false,
                'term' => str_replace(['\\?', '\\*'], ['?', '*'], $mask),
            ];
        }

        return $result;
    }

    public function processFunctionCall(FunctionCall $call)
    {
        $args = [];
        foreach ($call->getArgs() as $arg) {
            $args[] = $arg->onProcess($this);
        }

        return ['class' => 'func', 'name' => $call->getName(), 'args' => $args];
    }

    public function processOrder(Order $order)
    {
        $f = $order->getProperty()->onProcess($this);

        if ($f['class'] != 'field') {
            throw new \InvalidArgumentException('Only sorting by fields is supported currently');
        }

        return [
            $f['name'] => ['order' => $order->isAsc() ? 'asc' : 'desc'],
        ];
    }

    public function processAlias(Alias $alias)
    {
        $result = $alias->getExpression()->onProcess($this);
        $result['alias'] = $alias->getAlias();

        return $result;
    }

    private function buildAggregations($groups, $funcs)
    {
        if (!empty($funcs) && !empty($groups)) {
            $aggs = $this->buildFieldAggregations($funcs, $groups);
        } elseif (!empty($funcs) && empty($groups)) {
            $aggs = $this->buildGlobalAggregations($funcs);
        } elseif (empty($funcs) && !empty($groups)) {
            $aggs = $this->buildTermAggregations($groups);
        }

        if (!empty($aggs)) {
            $this->result['body']['aggs'] = $aggs;
        }
    }

    private function buildFieldAggregations($funcs, $groups)
    {
        //TODO Build bucketed aggregations based on groups
        $this->result['qb']['is-groupped-by'] = true;

        throw new \Exception("Grouping is not supported yet");
    }

    private function buildTermAggregations($groups)
    {
        //TODO Build bucketed aggregations based on groups
        $this->result['qb']['is-groupped-by'] = true;

        throw new \Exception("Grouping is not supported yet");

        /*
         foreach ($groups as $group) {
         $f = $group->onProcess($this);

         if ($f['class'] != 'field') {
         throw new \InvalidArgumentException('Only grouping by fields is supported currently');
         }

         $aggs[$f['agg-id']] = ['terms' => ['field' => $f['name']]];
         }
         */
    }

    private function buildGlobalAggregations($funcs)
    {
        $aggs = [];

        foreach ($funcs as $func) {
            if ($func['name'] == 'count' && empty($func['args'])) {
                $this->result['qb']['agg-path'][$func['agg-id']] = ['hits', 'total'];
            } else {
                $field = reset($func['args']);
                $aggs[$func['agg-id']] = [$this->mapFuncName($func['name']) => ['field' => $field['name']]];
                $this->result['qb']['agg-path'][$func['agg-id']] = ['aggregations', $func['agg-id'], 'value'];
            }
        }

        return $aggs;
    }

    private function mapFuncName($funcName)
    {
        if ($funcName == 'count') {
            return 'value_count';
        }

        return $funcName;
    }

    private function buildCriteria($criteria)
    {
        if ($criteria !== null) {
            $c = $criteria->onProcess($this);

            if ($c['class'] == 'filter') {
                $this->result['body']['query']['filtered']['filter'] = $c['term'];
            } else {
                $this->result['body']['query'] = $c['term'];
            }
        }
    }

    private function buildOrders($orders)
    {
        if (!empty($orders)) {
            foreach ($orders as $order) {
                $o = $order->onProcess($this);

                $this->result['body']['sort'][] = $o;
            }
        }
    }

    private function buildPagination($limit, $offset)
    {
        if (is_null($limit)) {
            if (!is_null($offset)) {
                throw new \InvalidArgumentException('Offset can not be set if the limit is ommited');
            }

            return;
        }

        if (!is_numeric($limit)) {
            throw new \InvalidArgumentException('Limit value must be numeric');
        } else {
            $this->result['size'] = $limit;
        }

        if (!is_null($offset)) {
            if (!is_numeric($offset)) {
                throw new \InvalidArgumentException('Offset value must be numeric');
            }
            $this->result['from'] = $offset;
        }
    }

    private function onInvalidMnemonic($type, $mnemonic)
    {
        throw new \UnexpectedValueException(
            "Unsupported {$type} operator mnemonic: '{$mnemonic}'"
        );
    }

    private function negateQuery($query)
    {
        if ($query['class'] == 'filter') {
            $query['term'] = ['not' => $query['term']];
        } elseif (!empty($query['term']['bool'])) {
            $old = $query['term']['bool'];
            $new = ['should' => [], 'must_not' => []];
            if (!empty($old['should'])) {
                $new['must_not'] = array_merge($new['must_not'], $old['should']);
            }
            if (!empty($old['must'])) {
                $new['must_not'] = array_merge($new['must_not'], $old['must']);
            }

            if (!empty($old['must_not'])) {
                $new['should'] = array_merge($new['should'], $old['must_not']);
            }

            $query['term']['bool'] = array_filter($new);
        } else {
            $query['term'] = ['bool' => ['must_not' => $query['term']]];
        }

        return $query;
    }

    private function buildDoc($fields, $values)
    {
        $result = ['id' => null, 'doc' => []];
        $cnt = count($fields);

        for ($i = 0; $i < $cnt; $i++) {
            $f = $fields[$i]->onProcess($this);
            $v = $values[$i]->onProcess($this);

            if ($f['name'] == 'id') {
                $result['id'] = $v['term'];
                continue;
            }

            $target = &$result['doc'];
            foreach (explode('.', $f['name']) as $part) {
                $target = &$target[$part];
            }
            $target = $v['term'];
        }

        return $result;
    }
}
