<?php declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\QueryBuilder\Step;

use Improved as i;
use Improved\Iterator\CombineIterator;
use Jasny\DB\Exception\InvalidFilterException;
use Jasny\DB\Mongo\QueryBuilder\Update\FilterComposer;
use Jasny\DB\Mongo\QueryBuilder\FilterQuery;
use OverflowException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jasny\DB\Mongo\QueryBuilder\Update\AbstractComposer
 * @covers \Jasny\DB\Mongo\QueryBuilder\Update\FilterComposer
 */
class FilterComposerTest extends TestCase
{
    public function provider()
    {
        return [
            ['', 42],
            ['not', ['$ne' => 42]],
            ['min', ['$gte' => 42]],
            ['max', ['$lte' => 42]],
            ['any', ['$in' => 42]],
            ['none', ['$nin' => 42]],
            ['all', ['$all' => 42]]
        ];
    }

    /**
     * @dataProvider provider
     */
    public function test(string $operator, $expected)
    {
        $input = new CombineIterator([['field' => 'foo', 'operator' => $operator]], [42]);

        $compose = new FilterComposer();

        $iterator = $compose($input);
        $this->assertInstanceOf(\Iterator::class, $iterator);

        ['keys' => $info, 'values' => $callbacks] = i\iterable_separate($iterator);

        $this->assertCount(1, $info);
        $this->assertEquals(['field' => 'foo', 'operator' => $operator, 'value' => 42], $info[0]);

        $this->assertCount(1, $callbacks);
        $this->assertInstanceOf(\Closure::class, $callbacks[0]);

        $query = $this->createMock(FilterQuery::class);
        $query->expects($this->once())->method('add')->with(['foo' => $expected]);

        ($callbacks[0])($query, 'foo', $operator, 42);
    }

    public function testIterate()
    {
        $info =[
            ['field' => 'foo', 'operator' => ''],
            ['field' => 'bar', 'operator' => 'min'],
            ['field' => 'color', 'operator' => 'not']
        ];

        $values = [42, 99, 'blue'];

        $input = new CombineIterator($info, $values);

        $compose = new FilterComposer();

        $iterator = $compose($input);
        $this->assertInstanceOf(\Iterator::class, $iterator);

        ['keys' => $info, 'values' => $callbacks] = i\iterable_separate($iterator);

        $this->assertCount(3, $info);
        $this->assertCount(3, $callbacks);
    }

    public function invalidInfoProvider()
    {
        return [
            ['$min', '', 42, "Invalid field '\$min': Starting with '$' isn't allowed"],
            ['foo.$min', '', 42, "Invalid field 'foo.\$min': Starting with '$' isn't allowed"],
            ['foo', 'dance', 42, "Invalid field 'foo (dance)': Unknown operator 'dance'"],
            ['foo', 'any', ['$min' => 10], "Invalid filter value for 'foo (any)': "
                . "Illegal array key '\$min', starting with '$' isn't allowed"],
            ['foo', 'any', [(object)['bar' => 22], (object)['$max' => 10]], "Invalid filter value for 'foo (any)': "
                . "Illegal object property '\$max', starting with '$' isn't allowed"]
        ];
    }

    /**
     * @dataProvider invalidInfoProvider
     */
    public function testInvalidFieldName(string $field, string $operator, $value, string $exceptionMsg)
    {
        $this->expectException(InvalidFilterException::class);
        $this->expectExceptionMessage($exceptionMsg);

        $input = new CombineIterator([['field' => $field, 'operator' => $operator]], [$value]);
        $compose = new FilterComposer();

        $iterator = $compose($input);

        ['values' => $callbacks] = i\iterable_separate($iterator);

        $query = $this->createMock(FilterQuery::class);
        ($callbacks[0])($query, $field, $operator, $value);
    }

    public function testRecursionCircularReference()
    {
        $this->expectException(OverflowException::class);
        $this->expectExceptionMessage("Unable to apply 'foo'; possible circular reference");

        $objectA = new \stdClass();
        $objectB = new \stdClass();

        $objectA->b = $objectB;
        $objectB->a = $objectA;

        $input = new CombineIterator([['field' => 'foo', 'operator' => '']], [$objectA]);

        $compose = new FilterComposer();
        $iterator = $compose($input);

        ['values' => $callbacks] = i\iterable_separate($iterator);

        $query = $this->createMock(FilterQuery::class);
        ($callbacks[0])($query, 'foo', '', $objectA);
    }
}
