<?php declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\QueryBuilder;

use Jasny\DB\Exception\InvalidOptionException;
use Jasny\DB\Mongo\QueryBuilder\OptionConverter;
use Jasny\DB\Option as opt;
use Jasny\DB\Option\OptionInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jasny\DB\Mongo\QueryBuilder\OptionConverter
 */
class OptionConverterTest extends TestCase
{
    public function testFields()
    {
        $option = opt\fields('foo', 'bar', 'qux');

        $converter = new OptionConverter();
        $result = $converter->convert([$option]);

        $this->assertEquals(['projection' => ['foo' => 1, 'bar' => 1, 'qux' => 1]], $result);
    }

    public function testOmit()
    {
        $option = opt\omit('foo', 'bar', 'qux');

        $converter = new OptionConverter();
        $result = $converter->convert([$option]);

        $this->assertEquals(['projection' => ['foo' => 0, 'bar' => 0, 'qux' => 0]], $result);
    }

    public function limitProvider()
    {
        return [
            [['limit' => 10], opt\limit(10)],
            [['limit' => 10, 'skip' => 40], opt\limit(10, 40)],
            [['limit' => 10, 'skip' => 40], opt\page(5, 10)]
        ];
    }

    /**
     * @dataProvider limitProvider
     */
    public function testLimit($expected, OptionInterface $option)
    {
        $converter = new OptionConverter();
        $result = $converter->convert([$option]);

        $this->assertEquals($expected, $result);
    }

    public function testSort()
    {
        $option = opt\sort('foo', 'bar', '~qux');

        $converter = new OptionConverter();
        $result = $converter->convert([$option]);

        $this->assertEquals(['sort' => ['foo' => 1, 'bar' => 1, 'qux' => -1]], $result);
    }


    public function testMultipleOpts()
    {
        $opts = [
            opt\fields('foo', 'bar'),
            opt\fields('color'),
            opt\omit('bar', 'qux'),
            opt\limit(10),
            opt\page(3, 20)
        ];

        $converter = new OptionConverter();
        $result = $converter->convert($opts);

        $expected = [
            'projection' => ['foo' => 1, 'bar' => 0, 'color' => 1, 'qux' => 0],
            'limit' => 20,
            'skip' => 40
        ];

        $this->assertEquals($expected, $result);
    }

    public function testInvoke()
    {
        $option = opt\fields('foo', 'bar', 'qux');

        $convert = new OptionConverter();
        $result = $convert([$option]);

        $this->assertEquals(['projection' => ['foo' => 1, 'bar' => 1, 'qux' => 1]], $result);
    }


    public function testUnsupportedOption()
    {
        $this->expectException(InvalidOptionException::class);
        $this->expectExceptionMessage("Unsupported query option class 'UnsupportedOption'");

        $option = $this->getMockBuilder(OptionInterface::class)
            ->setMockClassName('UnsupportedOption')
            ->disableOriginalConstructor()
            ->disableProxyingToOriginalMethods()
            ->getMock();

        $converter = new OptionConverter();
        $converter->convert([$option]);
    }
}
