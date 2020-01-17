<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\FieldMap;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\FieldMap\FieldMapInterface;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Update\UpdateInstruction;
use MongoDB\BSON\ObjectId;

/**
 * Mapping for vermongo style versioning collection.
 * @see https://github.com/thiloplanz/v7files/wiki/Vermongo
 */
class Vermongo implements FieldMapInterface
{
    protected FieldMapInterface $map;

    /**
     * Vermongo constructor.
     *
     * @param FieldMapInterface $map  Wrapped field map
     */
    public function __construct(FieldMapInterface $map)
    {
        $this->map = $map;
    }

    /**
     * @inheritDoc
     */
    public function toDB(string $appField): string
    {
        $dbField = $this->map->toDB($appField);

        return $dbField === '_id' || $dbField === '_version'
            ? "_id.$dbField"
            : $dbField;
    }

    /**
     * @inheritDoc
     */
    public function fromDB(string $dbField): string
    {
        if ($dbField === '_id._id' || $dbField === '_id._version') {
            $dbField = substr($dbField, 4);
        }

        return $this->map->fromDB($dbField);
    }

    /**
     * @inheritDoc
     */
    public function applyToFilter(array $filterItems): array
    {
        return Pipeline::with($filterItems)
            ->then([$this->map, 'applyToFilter'])
            ->map(static function (FilterItem $item) {
                return $item->getField() === '_id' || $item->getField() === '_version'
                    ? new FilterItem('_id.' . $item->getField(), $item->getOperator(), $item->getValue())
                    : $item;
            })
            ->toArray();
    }

    /**
     * @inheritDoc
     */
    public function applyToUpdate(array $update): array
    {
        $instructions = $this->map->applyToUpdate($update);

        foreach ($instructions as &$instruction) {
            $pairs = Pipeline::with($instruction->getPairs())
                ->mapKeys(fn($_, string $key) => $key === '_id' || $key === '_version' ? "_id.$key" : $key);

            if ($instruction->getPairs() !== $pairs) {
                $instruction = new UpdateInstruction($instruction->getOperator(), $pairs);
            }
        }

        return $instructions;
    }

    /**
     * @inheritDoc
     */
    public function applyToResult(iterable $result): iterable
    {
        return Pipeline::with($result)
            ->map(static function ($item) {
                if (is_object($item) && isset($item->_id) && !$item->_id instanceof ObjectId) {
                    $id = $item->_id;
                    $item->_id = $id->_id;
                    $item->_version = $id->_version;
                }

                if (is_array($item) && isset($item['_id']) && !$item['_id'] instanceof ObjectId) {
                    $id = $item['_id'];
                    $item = $id + $item;
                }

                return $item;
            })
            ->then([$this->map, 'applyToResult']);
    }

    /**
     * @inheritDoc
     */
    public function applyToItems(iterable $items): iterable
    {
        return Pipeline::with($items)
            ->then([$this->map, 'applyToItems'])
            ->map(static function ($item) {
                if (is_object($item) && isset($item->_id) && $item->_id instanceof ObjectId) {
                    $copy = clone $item;
                    $copy->_id = (object)['_id' => $item->_id, '_version' => $item->_version ?? new ObjectId()];
                    unset($copy->_version);

                    return $copy;
                }

                if (is_array($item) && isset($item['_id']) && $item['_id'] instanceof ObjectId) {
                    $item['_id'] = ['_id' => $item['_id'], '_version' => $item['_version'] ?? new ObjectId()];
                }

                return $item;
            });
    }
}
