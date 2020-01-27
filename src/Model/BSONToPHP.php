<?php /** @noinspection PhpUnused, PhpDocSignatureInspection, PhpDocMissingReturnTagInspection */

declare(strict_types=1);

namespace Jasny\DB\Mongo\Model;

use Improved\IteratorPipeline\Pipeline;
use MongoDB\BSON\Binary;
use MongoDB\BSON\ObjectIdInterface;
use MongoDB\BSON\UTCDateTimeInterface;
use MongoDB\Model\BSONArray;
use MongoDB\Model\BSONDocument;
use Ramsey\Uuid\Uuid;

/**
 * Convert BSON types to PHP types.
 */
class BSONToPHP
{
    public const DEFAULT_STEPS = [
        'convertBSONDocument',
        'convertBSONArray',
        'convertObjectId',
        'convertUUID',
    ];

    /**
     * @var array<\Closure>
     */
    protected array $steps = [];

    /**
     * BSONToPHP constructor.
     *
     * @param array<string|\Closure>|null $steps
     */
    public function __construct(?array $steps = null)
    {
        foreach ($steps ?? static::DEFAULT_STEPS as $step) {
            $this->steps[] = is_string($step) ? \Closure::fromCallable([$this, $step]) : $step;
        }
    }

    /**
     * Invoke converter.
     *
     * @param iterable|mixed $value
     * @return iterable|mixed
     */
    public function __invoke($value)
    {
        return is_iterable($value) ? $this->convertAll($value) : $this->convert($value);
    }

    /**
     * Convert value to PHP type.
     *
     * @param mixed $value
     * @return mixed
     */
    public function convert($value)
    {
        foreach ($this->steps as $step) {
            $value = $step($value);
        }

        return $value;
    }

    /**
     * Convert all values to PHP types.
     *
     * @param array|iterable $values
     * @return array|iterable
     */
    public function convertAll(iterable $values): iterable
    {
        $pipeline = $values instanceof Pipeline ? $values : Pipeline::with($values);

        foreach ($this->steps as $step) {
            $pipeline->map($step);
        }

        return is_array($values) ? $pipeline->toArray() : $pipeline;
    }


    /**
     * Convert BSONDocument to stdClass.
     */
    protected function convertBSONDocument($value)
    {
        return $value instanceof BSONDocument
            ? (object)$this->convertAll($value)->toArray()
            : $value;
    }

    /**
     * Convert BSONDocument to stdClass.
     */
    protected function convertBSONArray($value)
    {
        return $value instanceof BSONArray
            ? $this->convertAll($value)->toArray()
            : $value;
    }

    /**
     * Convert BSON ObjectId to string.
     */
    protected function convertObjectId($value)
    {
        return $value instanceof ObjectIdInterface
            ? (string)$value
            : $value;
    }

    /**
     * Convert BSON Binary (type UUID) to Ramsey Uuid object.
     */
    protected function convertUUID($value)
    {
        return $value instanceof Binary && $value->getType() === Binary::TYPE_UUID
            ? Uuid::fromBytes($value->getData())
            : $value;
    }

    /**
     * Convert BSON UTCDateTime to DateTime.
     */
    protected function convertUTCDateTime($value)
    {
        return $value instanceof UTCDateTimeInterface
            ? $value->toDateTime()
            : $value;
    }
}
