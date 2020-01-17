<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Functional;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\FieldMap\ConfiguredFieldMap;
use Jasny\DB\FieldMap\FieldMapInterface;
use Jasny\DB\Mongo\FieldMap\Vermongo;
use Jasny\DB\Mongo\QueryBuilder\Compose\SaveComposer;
use Jasny\DB\Mongo\QueryBuilder\Finalize\ConflictResolution;
use Jasny\DB\Mongo\Reader;
use Jasny\DB\Mongo\Writer;
use Jasny\DB\Option as opts;
use Jasny\DB\QueryBuilder\SaveQueryBuilder;
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

    public function createBaseWriter(FieldMapInterface $map): Writer
    {
        $basicWriter = Writer::basic($map);

        $saveQueryBuilder = (new SaveQueryBuilder(new SaveComposer()))
            ->withPreparation(static function (iterable $items) use ($map) {
                return Pipeline::with($items)
                    ->apply(static function (object $item) {
                        $item->id ??= new ObjectId();
                        $item->version ??= new ObjectId();
                    })
                    ->then([$map, 'applyToItems']);
            })
            ->withFinalization(new ConflictResolution());

        return new Writer(
            $basicWriter->getQueryBuilder(),
            $basicWriter->getUpdateQueryBuilder(),
            $saveQueryBuilder,
            $basicWriter->getResultBuilder()
        );
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

        $map = new ConfiguredFieldMap(['_id' => 'id', '_version' => 'version']);
        $vermongoMap = new Vermongo($map);

        $baseWriter = $this->createBaseWriter($map)->forCollection($this->collection);
        $vermongoWriter = Writer::basic($vermongoMap)->forCollection($this->vermongo);

        $this->writer = (new MultiWrite($baseWriter, $vermongoWriter))->withLogging($this->logger);
        $this->reader = Reader::basic($vermongoMap)->forCollection($this->vermongo)->withLogging($this->logger);
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

        $result = $this->writer->save($item, [opts\apply_result()]);
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
