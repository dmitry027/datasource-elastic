<?php
use Vda\Datasource\DocumentOriented\Elastic\Repository;
use Vda\Datasource\DocumentOriented\Elastic\QueryBuilder;
use Vda\Query\Field;
use Vda\Query\Table;
use Vda\Util\Type;
use Vda\Query\Select;
use Vda\Query\Insert;
use Vda\Query\Upsert;
use Vda\Query\Delete;
use Vda\Query\Update;

require '../vendor/autoload.php';

class DVideo extends Table
{
    public $id;
    public $added;
    public $tags;
    public $name;
    public $description;
    public $cost;

    protected $_schema = 'riv';

    public function __construct()
    {
        $this->id = new Field(Type::STRING);
        $this->cost = new Field(Type::INTEGER);
        $this->added = new Field(Type::DATE);
        $this->tags = [
            'en' => new Field(Type::STRING, 'desc.en.tags'),
            'it' => new Field(Type::STRING, 'desc.it.tags'),
        ];

        $this->name = [
            'en' => new Field(Type::STRING, 'desc.en.name'),
            'it' => new Field(Type::STRING, 'desc.it.name'),
        ];

        $this->description = [
            'en' => new Field(Type::STRING, 'desc.en.description'),
            'it' => new Field(Type::STRING, 'desc.it.description'),
        ];

        parent::__construct('video', 't');
    }
}

$r = new Repository(Elasticsearch\ClientBuilder::create()->build());

$qb = new QueryBuilder();

$t = new DVideo();

$s = Select::select()
    ->from($t)
    ->where(
        $t->id->eq(777)
        //,
        //$t->name['en']->like('Testname2en testname1en')
        //$t->cost->lt(15)
        //$t->name['it']->like('bella sega... quanta sborra'),
        //, $t->added->gt(new \DateTime('2015-05-26 00:00:00'))
        //$t->tags['en']->notlike(['camminare'])
        //, $t->tags['en']->like(['test',"test_download_4"])
        //, $t->name['it']->notnull()
        //, $t->description['en']->like('test_download_test_d')
        //$t->int->gt(10),
        //$t->int->eq(17)
    )->limit(20)
    ->offset(0)
    ->orderBy($t->added->asc());

//var_dump($qb->build($s));

$start = microtime(true);
var_dump('select', $r->select($s));
printf("%0.5f", microtime(true) - $start);

$item = [
    'id' => 777,
    'cost'  => 10,
    'added' => new DateTimeImmutable(),
    'tags.en' => ['testtag1en', 'Testtag2en'],
    'tags.it' => ['testtag1it', 'Testtag2it'],
    'name.en' => 'Testname2en testname1en',
    'name.it' => 'Testname2it testname1it',
    'description.en' => 'Testdesc2en testdesc1en',
    'description.it' => 'Testdesc2it testdesc1it',
];
$item2 = array_merge($item, ['id' => 778]);

$i = Insert::insert()
    ->into($t)
    ->populate($item)
    ->addBatch()
    ->populate($item2);

//var_dump($qb->build($i));
var_dump('insert', $r->insert($i), $r->getLastInsertId());
sleep(1);
var_dump('select', $r->select($s));

$u = Upsert::upsert()
    ->into($t)
    ->insert($t->id, 777)
    ->insert($t->cost, 20)
    ->insert($t->added, new DateTimeImmutable())
    ->insert($t->tags['en'], ['entag1', 'entag2'])
    ->insert($t->name['en'], 'New name en')
    ->insert($t->description['en'], 'New description en')
    ->insert($t->tags['it'], ['entag1', 'entagit'])
    ->insert($t->name['it'], 'New name it')
    ->insert($t->description['it'], 'New description it')
    ->update($t->cost, 100);

//var_dump($qb->build($u));
var_dump('Upsert', $r->upsert($u), $r->getLastInsertId());
sleep(1);
var_dump('select', $r->select($s));

$u = Update::update($t)
->set($t->cost, 1000)
->where($t->id->eq(777));
//var_dump($qb->build($u));
var_dump('Update', $r->update($u));
sleep(1);
var_dump('select', $r->select($s));

$d = Delete::delete()
    ->from($t)
    ->where($t->id->eq(777));

//var_dump($qb->build($d));
var_dump('Delete', $r->delete($d));
sleep(1);
var_dump('select', $r->select($s));

