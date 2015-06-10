<?php
namespace Vda\Datasource\DocumentOriented\Elastic;

use Elasticsearch\Client;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Vda\Datasource\IRepository;
use Vda\Query\Select;
use Vda\Query\Insert;
use Vda\Query\Upsert;
use Vda\Query\Update;
use Vda\Query\Delete;
use Vda\Query\Alias;
use Vda\Query\Field;
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

        $result = $this->elastic->search($q);

        foreach ($result['hits']['hits'] as $doc) {
            $accumulator->accumulate($this->flattenDoc($select->getFields(), $doc));

            if ($accumulator->isFilled()) {
                break;
            }
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

    protected function flattenDoc(array $fields, array $doc)
    {
        $tuple = [];

        foreach ($fields as $i => $field) {
            if ($fields[$i] instanceof Alias) {
                $name = $fields[$i]->getAlias();
                $field = $field->getExpression();
            }

            if ($field instanceof Field) {
                $name = $field->getName();

                if ($name == 'id') {
                    $val = $doc['_id'];
                } else {
                    $val = $doc['_source'];
                    foreach (explode('.', $name) as $part) {
                        $val = $val[$part];
                    }
                }

                if ($field->getType() == Type::DATE && !is_null($val)) {
                    $tuple[$i] = new \DateTimeImmutable($val);
                } else {
                    $tuple[$i] = $val;
                }
            } else {
                //TODO Handle expression fields
                $tuple[$i] = null;
            }
        }

        return $tuple;
    }
}
