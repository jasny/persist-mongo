<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Map;

use ArrayIterator;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\Map\AssertMap;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\Option\OptionInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jasny\DB\Mongo\Map\AssertMap
 */
class AssertMapTest extends TestCase
{
    protected AssertMap $assertMap;

    /**
     * @var MapInterface&MockObject
     */
    protected $innerMap;

    public function setUp(): void
    {
        $this->innerMap = $this->createMock(MapInterface::class);
        $this->assertMap = new AssertMap($this->innerMap);
    }

    public function testWithOpts()
    {
        $innerWithOpts = $this->createMock(MapInterface::class);

        $opts = [
            $this->createMock(OptionInterface::class),
            $this->createMock(OptionInterface::class),
        ];

        $this->innerMap->expects($this->once())->method('withOpts')
            ->with($this->identicalTo($opts))
            ->willReturn($innerWithOpts);

        $mapWithOpts = $this->assertMap->withOpts($opts);

        $this->assertInstanceOf(AssertMap::class, $mapWithOpts);
        $this->assertNotSame($this->assertMap, $mapWithOpts);

        $this->assertSame($innerWithOpts, $mapWithOpts->getInner());
    }


    public function testApplyToFieldWithValidField()
    {
        $this->innerMap->expects($this->once())->method('applyToField')
            ->with('foo.bar')
            ->willReturn('fox.bar');

        $mapped = $this->assertMap->applyToField('foo.bar');

        $this->assertEquals('fox.bar', $mapped);
    }

    public function illegalFieldProvider()
    {
        return [
            '$set' => ['$set'],
            'foo.$[bar]' => ['foo.$[bar]'],
        ];
    }

    /**
     * @dataProvider illegalFieldProvider
     */
    public function testApplyToField(string $field)
    {
        $this->innerMap->expects($this->never())->method('applyToField');

        $this->expectException(\UnexpectedValueException::class);
        $this->expectExceptionMessage("Illegal field name '$field': starting with '$' isn't allowed");

        $this->assertMap->applyToField($field);
    }


    public function testApplyWithValidItem()
    {
        $item = ['foo' => 'bar', 'one' => 1, 'us$' => 200];

        $this->innerMap->expects($this->once())->method('apply')
            ->willReturn(['a' => 'b']);

        $mapped = $this->assertMap->apply($item);

        $this->assertEquals(['a' => 'b'], $mapped);
    }

    public function illegalItemProvider()
    {
        return [
            '$set' => [['a' => 'b', '$set' => 1], "Illegal field name '\$set': starting with '$' isn't allowed"],
            'foo.bar' => [['foo.bar' => 1], "Illegal field name 'foo.bar': may not contain a '.'"],
            'foo[$set]' => [
                ['foo' => ['$set' => 1]],
                "Illegal field name '\$set' in 'foo': starting with '$' isn't allowed",
            ],
            'foo[bar][$set]' => [
                ['foo' => ['bar' => ['$set' => 1]]],
                "Illegal field name '\$set' in 'foo.bar': starting with '$' isn't allowed",
            ],
            'object' => [(object)['$set' => 1], "Illegal field name '\$set': starting with '$' isn't allowed"]
        ];
    }

    /**
     * @dataProvider illegalItemProvider
     */
    public function testApply($item, string $message)
    {
        $this->innerMap->expects($this->never())->method('apply');

        $this->expectException(\UnexpectedValueException::class);
        $this->expectExceptionMessage($message);

        $this->assertMap->apply($item);
    }

    public function testApplyCircular()
    {
        $this->innerMap->expects($this->never())->method('apply');

        $this->expectException(\OverflowException::class);
        $this->expectExceptionMessage("Possible circular reference");

        $a = (object)[];
        $b = (object)[];
        $a->b = $b;
        $b->a = $a;

        $this->assertMap->apply($a);
    }


    public function testAsPreparation()
    {
        $prepare = AssertMap::asPreparation();
        $items = new ArrayIterator([]);

        $map = $this->createMock(MapInterface::class);

        $opts = [
            $this->createMock(OptionInterface::class),
            opt\setting('map', $map),
        ];

        $result = $prepare($items, $opts);

        $this->assertSame($result, $items);

        $this->assertIsArray($opts);
        $this->assertCount(2, $opts);

        $optsMap = opt\setting('map', null)->findIn($opts);

        $this->assertInstanceOf(AssertMap::class, $optsMap);
        $this->assertSame($map, $optsMap->getInner());
    }
}
