<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Functional;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\FieldMap;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\Map\Vermongo;
use Jasny\DB\Mongo\Save\SaveComposer;
use Jasny\DB\Mongo\Reader\Reader;
use Jasny\DB\Mongo\Writer\Finalize\DetermineMethod;
use Jasny\DB\Mongo\Writer\Writer;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\QueryBuilder\SaveQueryBuilder;
use Jasny\DB\Save\Prepare\MapItems;
use Jasny\DB\Writer\MultiWrite;
use MongoDB\BSON\ObjectId;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use Monolog\Handler\NullHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use function Jasny\objectify;

/**
 * Test versioning MongoDB data.
 * @see https://github.com/thiloplanz/v7files/wiki/Vermongo
 *
 * Note: uses ObjectId instead of integer for `_version`.
 *
 * @coversNothing
 */
class VermongoTest extends TestCase
{
    protected Database $db;
    protected Collection $collection;
    protected Collection $vermongo;
    protected MultiWrite $writer;
    protected Reader $reader;

    protected Logger $logger;

    public function createBaseWriter(MapInterface $map): Writer
    {
        $saveQueryBuilder = (new SaveQueryBuilder(new SaveComposer()))
            ->withPreparation(
                Pipeline::build()->apply(static function (object $item) {
                    $item->id ??= new ObjectId();
                    $item->version ??= new ObjectId();
                }),
                new MapItems(),
            )
            ->withFinalization(new DetermineMethod());

        return (new Writer($map))->withSaveQueryBuilder($saveQueryBuilder);
    }

    public function setUp(): void
    {
        $this->db = (new Client())->test;

        $typeMap = ['array' => 'array', 'document' => 'array', 'root' => 'array'];
        $this->collection = $this->db->selectCollection('foo', ['typeMap' => $typeMap]);
        $this->vermongo = $this->db->selectCollection('foo.vermongo', ['typeMap' => $typeMap]);

        $this->logger = getenv('JASNY_DB_TESTS_DEBUG') === 'on'
            ? new Logger('MongoDB', [new StreamHandler(STDERR)])
            : new Logger('MongoDB', [new NullHandler()]);

        $map = new FieldMap(['_id' => 'id', '_version' => 'version']);
        $vermongoMap = new Vermongo($map);

        $baseWriter = $this->createBaseWriter($map)->for($this->collection);
        $vermongoWriter = (new Writer($vermongoMap))->for($this->vermongo);

        $this->writer = (new MultiWrite($baseWriter, $vermongoWriter))->withLogging($this->logger);
        $this->reader = (new Reader($vermongoMap))->for($this->vermongo)->withLogging($this->logger);
    }

    public function tearDown(): void
    {
        /*if (isset($this->collection)) {
            $this->collection->drop();
        }
        if (isset($this->vermongo)) {
            $this->vermongo->drop();
        }*/
    }

    public function testSave()
    {
        $item = (object)['name' => 'john'];

        $result = $this->writer->save($item, [opt\apply_result()]);
        $record = $result->first();

        $this->assertIsObject($record);
        $this->assertObjectHasAttribute('id', $record);
        $this->assertObjectHasAttribute('version', $record);

        $this->assertInMongoCollection(
            $this->collection,
            ['name' => 'john', '_version' => $record->version],
            $record->id
        );
        $this->assertInMongoCollection(
            $this->vermongo,
            ['name' => 'john'],
            ['_id' => $record->id, '_version' => $record->version]
        );

        $fetched = $this->reader->fetch(['_id' => $record->id, '_version' => $record->version]);
        $this->assertEquals($record, objectify($fetched->first()));
    }

    protected function assertInMongoCollection(Collection $collection, $expected, $id)
    {
        $found = $collection->findOne(['_id' => $id]);

        if (!$found) {
            $this->fail(sprintf(
                "No document found with id %s in %s",
                json_encode($id),
                $collection->getCollectionName()
            ));
            return;
        }

        $this->assertEquals(['_id' => $id] + $expected, $found);
    }

    protected function assertNotInMongoCollection($id)
    {
        $found = $this->collection->findOne(['_id' => $id]);
        $this->assertNull($found);
    }
}
