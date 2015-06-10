<?php
namespace Vda\Datasource\DocumentOriented\Elastic\Query;

use Elasticsearch\Client;

abstract class ElasticQuery
{
    protected $params;

    public function __construct(array $params)
    {
        $this->params = $params;
    }

    abstract public function execute(Client $elastic);
}