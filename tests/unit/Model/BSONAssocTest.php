<?php declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Model;

use Jasny\DB\Mongo\Model\BSONAssoc;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jasny\DB\Mongo\Model\BSONAssoc
 */
class BSONAssocTest extends TestCase
{
    protected array $assoc = [
        'foo' => 42,
        'yoo-bar.color' => 'blue',
        'array' => ['hello', 'world'],
        'child1' => ['I' => 'one', 'II' => 'two'],
        'child2' => ['a' => 'AAA', 'b' => 'BBB'],
    ];

    protected array $document = [
        ['__key' => 'foo', '__value' => 42],
        ['__key' => 'yoo-bar.color', '__value' => 'blue'],
        ['__key' => 'array', '__value' => ['hello', 'world']],
        ['__key' => 'child1', 'I' => 'one', 'II' => 'two'],
        ['__key' => 'child2', 'a' => 'AAA', 'b' => 'BBB'],
    ];

    public function testBsonSerialize()
    {
        $this->assoc['child2'] = (object)$this->assoc['child2'];

        $bson = new BSONAssoc($this->assoc);
        $result = $bson->bsonSerialize();

        $this->assertEquals($this->document, $result);
    }

    public function testBsonDeserialize()
    {
        $bson = new BSONAssoc();
        $bson->bsonDeserialize($this->document);

        $this->assertEquals($this->assoc, $bson->getArrayCopy());
    }

    public function testJsonSerialize()
    {
        $bson = new BSONAssoc($this->assoc);
        $json = json_encode($bson);

        $this->assertJsonStringEqualsJsonString(json_encode($this->assoc), $json);
    }
}
