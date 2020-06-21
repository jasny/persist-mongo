<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Map;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Filter\FilterItem;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Update\UpdateInstruction;
use MongoDB\BSON\ObjectId;

/**
 * Mapping for vermongo style versioning collection.
 * @see https://github.com/thiloplanz/v7files/wiki/Vermongo
 */
class Vermongo implements MapInterface
{
    protected MapInterface $map;

    /**
     * Vermongo constructor.
     *
     * @param MapInterface|null $map  Wrapped field map
     */
    public function __construct(?MapInterface $map = null)
    {
        $this->map = $map ?? new NoMap();
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
                ->mapKeys(fn($_, string $key) => $key === '_id' || $key === '_version' ? "_id.$key" : $key)
                ->toArray();

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
                    $item->_version = $id->_version ?? null;
                }

                if (is_array($item) && isset($item['_id']) && !$item['_id'] instanceof ObjectId) {
                    $id = $item['_id'];
                    $item['_id'] = $id['_id'];
                    $item['_version'] = $id['_version'] ?? null;
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

    /**
     * @inheritDoc
     */
    public function withOpts(array $opts): MapInterface
    {
        return $this;
    }

    /**
     * @inheritDoc
     */
    public function applyToField(string $field)
    {
        // TODO: Implement applyToField() method.
    }

    /**
     * @inheritDoc
     */
    public function apply($item)
    {
        // TODO: Implement apply() method.
    }

    /**
     * @inheritDoc
     */
    public function applyInverse($item)
    {
        // TODO: Implement applyInverse() method.
    }
}
