<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Functional;

use Improved as i;
use Jasny\DB\Map\FieldMap;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Mongo\Query\Filter\PipelineStage;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Mongo\Reader\Reader;
use Jasny\DB\Mongo\Writer\Writer;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\Query\CustomOperator;
use Jasny\DB\Update\Functions as update;
use MongoDB\BSON\ObjectId;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use Monolog\Handler\NullHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use function Jasny\array_without;

/**
 * Test against the zips collection.
 *
 *   wget http://media.mongodb.org/zips.json
 *   mongoimport -v --file=zips.json
 *
 * @coversNothing
 */
class ZipTest extends TestCase
{
    protected Database $db;
    protected Collection $collection;
    protected Reader $reader;
    protected Writer $writer;

    protected Logger $logger;

    public function setUp(): void
    {
        $this->db = (new Client())->test;

        if (i\iterable_count($this->db->listCollections(['filter' => ['name' => 'zips']])) === 0) {
            $this->markTestSkipped("The 'zips' collection isn't present in the 'test' db");
        }

        $typeMap = ['array' => 'array', 'document' => 'array', 'root' => 'array'];
        $this->collection = $this->db->selectCollection('zips', ['typeMap' => $typeMap]);

        if (in_array('write', $this->getGroups(), true)) {
            $this->cloneCollection(); // Only clone if data is changed as cloning slows down the tests.
        }

        $this->logger = getenv('JASNY_DB_TESTS_DEBUG') === 'on'
            ? new Logger('MongoDB', [new StreamHandler(STDERR)])
            : new Logger('MongoDB', [new NullHandler()]);

        $map = new FieldMap(['id' => '_id']);

        $this->reader = (new Reader())
            ->for($this->collection)
            ->withMap($map)
            ->withLogging($this->logger);

        $this->writer = (new Writer())
            ->for($this->collection)
            ->withMap($map)
            ->withLogging($this->logger);
    }

    public function tearDown(): void
    {
        if (isset($this->db) && $this->db->getDatabaseName() === 'test_jasnydb') {
            $this->db->drop();
        }
    }

    protected function cloneCollection(): void
    {
        $name = $this->collection->getCollectionName() . '_copy';
        $this->collection->aggregate([['$out' => $name]]);

        $this->collection = $this->db->selectCollection($name, ['typeMap' => $this->collection->getTypeMap()]);
    }


    public function testCount()
    {
        $this->assertEquals(29353, $this->reader->count());
        $this->assertEquals(1595, $this->reader->count(['state' => "NY"]));
    }

    public function testFetchFirst()
    {
        $location = $this->reader->fetch(['id' => "01008"])->first();

        $this->assertEquals([
            "id" => "01008",
            "city" => "BLANDFORD",
            "loc" => [-72.936114, 42.182949],
            "pop" => 1240,
            "state" => "MA"
        ], $location);
    }

    public function testFetchLimit()
    {
        $expected = [
            ["id" => "06390", "city" => "FISHERS ISLAND"],
            ["id" => "10001", "city" => "NEW YORK"],
            ["id" => "10002", "city" => "NEW YORK"],
            ["id" => "10003", "city" => "NEW YORK"],
            ["id" => "10004", "city" => "GOVERNORS ISLAND"],
        ];

        $result = $this->reader->fetch(
            ['state' => "NY"],
            opt\limit(5),
            opt\sort('id'),
            opt\fields('id', 'city'),
        );

        $locations = i\iterable_to_array($result);

        $this->assertEquals($expected, $locations);
    }

    /**
     * Add a custom filter to support `(near)` as operator.
     *
     * Using $geoWithin instead of $near, because of countDocuments.
     * @see https://docs.mongodb.com/manual/reference/method/db.collection.countDocuments/#query-restrictions
     */
    public function testNearFilter()
    {
        $this->collection->createIndex(['loc' => "2d"]);

        $this->reader = $this->reader->withComposer(
            new CustomOperator(
                'near',
                static function (FilterQuery $query, FilterItem $filter, array $opts) {
                    [$field, $value] = [$filter->getField(), $filter->getValue()];
                    $dist = opt\setting('near', 1.0)->findIn($opts);

                    $query->match([$field => ['$geoWithin' => ['$center' => [$value, $dist]]]]);
                }
            ),
            $this->reader->getComposer(),
        );

        $filter = ['loc(near)' => [-72.622739, 42.070206]];

        $this->assertEquals(445, $this->reader->count($filter));
        $this->assertEquals(14, $this->reader->count($filter, opt\setting('near', 0.1)));
    }

    public function testFetchWithCustomFieldMap()
    {
        $expected = [
            ["zipcode" => "06390", "city" => "FISHERS ISLAND"],
            ["zipcode" => "10001", "city" => "NEW YORK"],
            ["zipcode" => "10002", "city" => "NEW YORK"],
            ["zipcode" => "10003", "city" => "NEW YORK"],
            ["zipcode" => "10004", "city" => "GOVERNORS ISLAND"],
        ];

        $map = new FieldMap([
            'zipcode' => '_id',
            'city' => 'city',
            'area' => 'state',
        ]);

        $this->reader = (new Reader())->withMap($map)->for($this->collection)->withLogging($this->logger);

        $result = $this->reader->fetch(
            ['area' => "NY"],
            opt\limit(5),
            opt\sort('zipcode'),
            opt\omit('area', 'loc', 'pop'),
        );

        $locations = i\iterable_to_array($result);
        $this->assertEquals($expected, $locations);
    }

    /**
     * @group write
     */
    public function testSaveAll()
    {
        $locations = [
            'a' => ["id" => "90208", "city" => "BLUE HILLS", "loc" => [-118.406477, 34.092], "state" => "CA"],
            '2' => ["id" => "90209", "city" => "BLUE HILLS", "loc" => [-118.407, 34.0], "pop" => 99, "state" => "CA"],
        ];

        $result = $this->writer->saveAll($locations);

        foreach ($result as $key => $document) {
            $location = $locations[$key];

            $expected = ['_id' => $location['id']] + array_diff_key($location, ['id' => 0]);
            $this->assertInMongoCollection($expected, $document->id);
        }
    }

    /**
     * @group write
     */
    public function testSaveAllWithGeneratedIds()
    {
        $locations = [
            'a' => (object)["city" => "BLUE HILLS", "loc" => [-118.406477, 34.092], "state" => "CA"],
            '2'  => (object)["city" => "BLUE HILLS", "loc" => [-118.407, 34.0], "pop" => 99, "state" => "CA"],
        ];

        $result = $this->writer->saveAll($locations, opt\apply_result());

        foreach ($result as $key => $location) {
            $this->assertArrayHasKey($key, $locations);
            $this->assertSame($locations[$key], $location);
            $this->assertObjectHasAttribute('id', $location);
            $this->assertInstanceOf(ObjectId::class, $location->id);

            $expected = ['_id' => $location->id] + array_diff_key((array)$location, ['id' => 0]);
            $this->assertInMongoCollection($expected, $location->id);
        }
    }

    /**
     * @group write
     */
    public function testSaveAllWithCustomFieldMap()
    {
        $map = new FieldMap([
            'ref' => '_id',
            'city' => 'city',
            'latlon' => 'loc',
            'area' => 'state'
        ]);

        $this->writer = (new Writer())
            ->withMap($map)
            ->for($this->collection)
            ->withLogging($this->logger);

        $locations = [
            'a' => (object)[
                "city" => "BLUE HILLS",
                "latlon" => [-118.406477, 34.092],
                "area" => "CA",
            ],
            '2' => (object)[
                "city" => "BLUE HILLS",
                "latlon" => [-118.407, 34.0],
                "pop" => 99,
                "area" => "CA",
                "bar" => 'r',
            ],
        ];

        $result = $this->writer->saveAll($locations, opt\apply_result());

        foreach ($result as $key => $location) {
            $this->assertArrayHasKey($key, $locations);
            $this->assertSame($locations[$key], $location);
            $this->assertObjectHasAttribute('ref', $location);
            $this->assertInstanceOf(ObjectId::class, $location->ref);

            $expected = [
                '_id' => $location->ref,
                'city' => $location->city,
                'loc' => $location->latlon,
                'state' => $location->area,
            ] + array_without((array)$location, ['ref', 'city', 'latlon', 'area']);

            $this->assertInMongoCollection($expected, $location->ref);
        }
    }


    /**
     * @group write
     */
    public function testUpdate()
    {
        $result = $this->writer->update(["_id" => "10004"], update\set(["city" => "NEW YORK"]));

        $this->assertEquals(1, $result->getMeta('count'));

        $expected = [
            '_id' => "10004",
            'city' => "NEW YORK",
            'loc' => [-74.019025, 40.693604],
            'pop' => 3593,
            'state' => "NY",
        ];

        $this->assertInMongoCollection($expected, "10004");
    }

    protected function initWriterWithNear(): void
    {
        $this->collection->createIndex(['loc' => "2d"]);

        $this->writer = $this->writer->withComposer(
            new CustomOperator(
                'near',
                static function (FilterQuery $query, FilterItem $filter, array $opts) {
                    [$field, $value] = [$filter->getField(), $filter->getValue()];
                    $dist = opt\setting('near', 1.0)->findIn($opts);

                    $query->match([$field => ['$geoWithin' => ['$center' => [$value, $dist]]]]);
                }
            ),
            $this->writer->getComposer(),
        );
    }

    /**
     * @group write
     */
    public function testUpdateNear()
    {
        $this->initWriterWithNear();

        $result = $this->writer->update(
            ['loc(near)' => [-72.622739, 42.070206]],
            update\set(["state" => "AD"]),
            opt\setting('near', 0.1),
        );

        $this->assertEquals(14, $result->getMeta('count'));
        $this->assertEquals(14, $this->reader->count(["state" => "AD"]));
    }

    /**
     * @group write
     */
    public function testUpdateWithLimit()
    {
        $result = $this->writer->update(
            ["city" => "NEW YORK"],
            update\set(["city" => "NEW YORK CITY"]),
            opt\limit(1),
        );

        $this->assertEquals(1, $result->getMeta('count'));
        $this->assertEquals(1, $this->reader->count(["city" => "NEW YORK CITY"]));
    }


    /**
     * @group write
     */
    public function testDelete()
    {
        $result = $this->writer->delete(["_id" => "10004"]);

        $this->assertEquals(1, $result->getMeta('count'));

        $this->assertNotInMongoCollection("10004");
    }

    /**
     * @group write
     */
    public function testDeleteNear()
    {
        $this->initWriterWithNear();

        $countBefore = $this->reader->count();

        $result = $this->writer->delete(
            ['loc(near)' => [-72.622739, 42.070206]],
            opt\setting('near', 0.1),
        );

        $this->assertEquals(14, $result->getMeta('count'));

        $removedCount = $countBefore - $this->reader->count();
        $this->assertEquals(14, $removedCount);
    }

    /**
     * @see https://docs.mongodb.com/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
     */
    public function testAggregation()
    {
        $this->reader = $this->reader
            ->withMap(new FieldMap(['state' => '_id']))
            ->withComposer(
                new PipelineStage('$group', ['_id' => '$state', 'totalPop' => ['$sum' => '$pop']]),
                $this->reader->getComposer(),
            );

        $result = $this->reader->fetch(['totalPop(min)' => 10*1000*1000], opt\sort('~totalPop'));

        $expected = [
            ["state" => "CA", "totalPop" => 29754890],
            ["state" => "NY", "totalPop" => 17990402],
            ["state" => "TX", "totalPop" => 16984601],
            ["state" => "FL", "totalPop" => 12686644],
            ["state" => "PA", "totalPop" => 11881643],
            ["state" => "IL", "totalPop" => 11427576],
            ["state" => "OH", "totalPop" => 10846517],
        ];

        $this->assertEquals($expected, i\iterable_to_array($result));
    }

    /**
     * @see https://docs.mongodb.com/manual/tutorial/aggregation-zip-code-data-set/#return-largest-and-smallest-cities-by-state
     */
    public function testComplexAggregation()
    {
        $this->reader = $this->reader
            ->withMap(new FieldMap(['state' => '_id']))
            ->withComposer(
                new PipelineStage('$group', [
                    '_id' => ['state' => '$state', 'city' => '$city'],
                    'pop' => ['$sum' => '$pop']
                ]),
                new PipelineStage('$sort', ['pop' => 1]),
                new PipelineStage('$group', [
                    '_id' => '$_id.state',
                    'biggestCity' => ['$last' => '$_id.city'],
                    'biggestPop' => ['$last' => '$pop'],
                    'smallestCity' => ['$first' => '$_id.city'],
                    'smallestPop' => ['$first' => '$pop'],
                ]),
                new PipelineStage('$project', [
                    '_id' => '$_id',
                    'biggestCity' => ['name' => '$biggestCity', 'pop' => '$biggestPop'],
                    'smallestCity' => ['name' => '$smallestCity', 'pop' => '$smallestPop'],
                ]),
                $this->reader->getComposer(),
            );

        $result = $this->reader->fetch(['state' => 'WA'])->first();

        $expected = [
            'state' => "WA",
            'biggestCity' => ['name' => "SEATTLE", 'pop' => 520096],
            'smallestCity' => ['name' => 'BENGE', 'pop' => 2],
        ];

        $this->assertEquals($expected, i\iterable_to_array($result));
    }


    protected function assertInMongoCollection($expected, $id)
    {
        $found = $this->collection->findOne(['_id' => $id]);

        if (!$found) {
            $this->fail("No document found with id '$id' in MongoDB collection");
            return;
        }

        $this->assertEquals($expected, $found);
    }

    protected function assertNotInMongoCollection($id)
    {
        $found = $this->collection->findOne(['_id' => $id]);
        $this->assertNull($found);
    }
}
