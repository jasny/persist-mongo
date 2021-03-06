<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Option\FieldsOption;
use Jasny\DB\Option\Functions as opt;
use Jasny\DB\Option\OptionInterface;

/**
 * Methods to convert FieldsOption to a MongoDB '$project' stage.
 */
trait ProjectionTrait
{
    /**
     * Traverse through the options and calculate the MongoDB projection.
     *
     * @param OptionInterface[] $opts
     * @return array<string,int>
     */
    protected function getProjectionFromOpts(array $opts): array
    {
        $projection = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof FieldsOption)
            ->map(fn(FieldsOption $opt) => $this->project($opt->getFields(), $opt->isNegated(), $opts))
            ->flatten(true)
            ->toArray();

        if ($projection === []) {
            return [];
        }

        // MongoDB will include `_id` if not specified.
        if (!isset($projection['_id']) && i\iterable_has_any($projection, fn($value) => $value !== 0)) {
            $projection['_id'] = 0;
        }

        return $projection;
    }

    /**
     * Convert fields/omit opt to MongoDB projection option.
     *
     * @param string[]          $fields
     * @param bool              $negate
     * @param OptionInterface[] $opts
     * @return iterable<string,int>
     */
    protected function project(array $fields, bool $negate, array $opts): iterable
    {
        $map = opt\setting('map', new NoMap())->findIn($opts, MapInterface::class);

        return Pipeline::with($fields)
            ->typeCheck('string', new \UnexpectedValueException())
            ->flip()
            ->fill($negate ? 0 : 1)
            ->mapKeys(fn($_, string $field) => $map->applyToField($field) ?? $field);
    }
}
