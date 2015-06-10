<?php
namespace Vda\Datasource\DocumentOriented\Elastic\Query;

use Elasticsearch\Client;

class Select extends ElasticQuery
{
    public function execute(Client $elastic)
    {
        //TODO Use $elatic->get() if only the primary key used in query
        return $elastic->search($this->params);
    }
}
