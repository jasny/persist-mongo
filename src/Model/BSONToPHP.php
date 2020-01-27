<?php /** @noinspection PhpUnused, PhpDocSignatureInspection, PhpDocMissingReturnTagInspection */

declare(strict_types=1);

namespace Jasny\DB\Mongo\Model;

use Improved\IteratorPipeline\Pipeline;
use MongoDB\BSON\Binary;
use MongoDB\BSON\ObjectIdInterface;
use MongoDB\BSON\Type as BSONType;
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
        'convertUTCDateTime',
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
     * Convert value.
     *
     * @param mixed $value
     * @return mixed
     */
    public function __invoke($value)
    {
        foreach ($this->steps as $step) {
            if (!$value instanceof BSONType) {
                break;
            }

            $value = $step($value);
        }

        return $value;
    }

    /**
     * Convert BSONDocument to stdClass.
     */
    protected function convertBSONDocument($value)
    {
        return $value instanceof BSONDocument
            ? (object)Pipeline::with($value)->map($this)->toArray()
            : $value;
    }

    /**
     * Convert BSONDocument to stdClass.
     */
    protected function convertBSONArray($value)
    {
        return $value instanceof BSONArray
            ? Pipeline::with($value)->map($this)->toArray()
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
