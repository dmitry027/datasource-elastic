<?php
namespace Vda\Datasource\DocumentOriented\Elastic;

use Elasticsearch\Client;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Vda\Datasource\IRepository;
use Vda\Query\Delete;
use Vda\Query\Field;
use Vda\Query\Insert;
use Vda\Query\Select;
use Vda\Query\Update;
use Vda\Query\Upsert;
use Vda\Util\Type;

class Repository implements IRepository, LoggerAwareInterface
{
    private $elastic;
    private $qb;
    private $logger;
    private $lastInsertId;

    public function __construct(Client $elastic)
    {
        $this->elastic = $elastic;
        $this->qb = new QueryBuilder();
    }

    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function select(Select $select)
    {
        $accumulator = $select->getResultAccumulator();
        $accumulator->reset($select->getProjection());

        $q = $this->qb->build($select);

        if ($this->logger) {
            $this->logger->debug("Search: {params}", ['params' => $q]);
        }

        $meta = $q['qb'];
        unset($q['qb']);
        $result = $this->elastic->search($q);

        if (empty($meta['is-groupped-by']) && empty($meta['has-fields'])) {
            $accumulator->accumulate($this->flattenDoc($meta, [], $result));
        } elseif (empty($meta['is-groupped-by']) && !empty($meta['has-fields'])) {
            foreach ($result['hits']['hits'] as $hit) {
                $accumulator->accumulate($this->flattenDoc($meta, $hit, $result));

                if ($accumulator->isFilled()) {
                    break;
                }
            }
        } else {
            //TODO Implement groupped queries
        }

        return $accumulator->getResult();
    }

    public function insert(Insert $insert)
    {
        $q = $this->qb->build($insert);

        if ($this->logger) {
            $this->logger->debug("Insert: {params}", ['params' => $q]);
        }

        $res = $this->elastic->bulk($q);

        if ($this->logger) {
            $this->logger->debug("Result: {result}", ['result' => $res]);
        }

        $this->lastInsertId = null;

        $inserted = 0;
        foreach ($res['items'] as $i) {
            if (in_array($i['create']['status'], [200, 201])) {
                $this->lastInsertId = $i['create']['_id'];
                $inserted++;
            }
        }

        return $inserted;
    }

    public function upsert(Upsert $upsert)
    {
        $q = $this->qb->build($upsert);

        if ($this->logger) {
            $this->logger->debug("Upsert: {params}", ['params' => $q]);
        }

        $res = $this->elastic->bulk($q);
        if ($this->logger) {
            $this->logger->debug("Result: {result}", ['result' => $res]);
        }

        $this->lastInsertId = null;

        $affected = 0;
        foreach ($res['items'] as $i) {
            $section = empty($i['update']) ? 'create' : 'update';
            if (in_array($i[$section]['status'], [200, 201])) {
                $this->lastInsertId = $i[$section]['_id'];
                $affected++;
            }
        }

        return $affected;
    }

    public function update(Update $update)
    {
        $q = $this->qb->build($update);

        if ($this->logger) {
            $this->logger->debug("Update: {params}", ['params' => $q]);
        }

        $res = $this->elastic->bulk($q);

        $updated = 0;
        foreach ($res['items'] as $i) {
            if ($i['update']['status'] == 200) {
                $updated++;
            }
        }

        return $updated;
    }

    public function delete(Delete $delete)
    {
        $q = $this->qb->build($delete);

        if ($this->logger) {
            $this->logger->debug("Delete: {params}", ['params' => $q]);
        }

        $this->elastic->deleteByQuery($q);

        return true;
    }

    public function getLastInsertId()
    {
        return $this->lastInsertId;
    }

    protected function flattenDoc(array $meta, array $currentHit, array $fullResult)
    {
        $tuple = [];
        foreach ($meta['fields'] as $i => $field) {
            if ($field['class'] == 'field') {
                if ($field['name'] == 'id') {
                    $val = $currentHit['_id'];
                } else {
                    $val = $this->getIn($currentHit['_source'], explode('.', $field['name']));
                }

                if ($field['type'] == Type::DATE && !is_null($val)) {
                    $tuple[$i] = new \DateTimeImmutable($val);
                } else {
                    $tuple[$i] = $val;
                }
            } elseif ($field['class'] == 'func') {
                if (!empty($field['agg-id'])) {
                    $tuple[$i] = $this->getIn($fullResult, $meta['agg-path'][$field['agg-id']]);
                } else {
                    //TODO Handle non-aggregate function call
                }
            } else {
                //TODO Handle expression fields
                $tuple[$i] = null;
            }
        }

        return $tuple;
    }

    private function getIn($coll, $path)
    {
        $val = $coll;
        foreach ($path as $part) {
            $val = isset($val[$part]) ? $val[$part] : null;
        }

        return $val;
    }
}
