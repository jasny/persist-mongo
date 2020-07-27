<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\FieldsOption;
use Jasny\DB\Query\ComposerInterface;

/**
 * Apply fields and omit query option(s) to a MongoDB query.
 *
 * @implements ComposerInterface<FilterQuery,FilterItem>
 */
class ApplyProjection implements ComposerInterface
{
    use ProjectionTrait;

    /**
     * @inheritDoc
     */
    public function compose(object $accumulator, iterable $items, array $opts = []): void
    {
        $this->finalize($accumulator, $opts);
    }

    /**
     * @inheritDoc
     */
    public function prepare(iterable $items, array &$opts = []): iterable
    {
        return $items;
    }

    /**
     * @inheritDoc
     */
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        return $items;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
        $projection = $this->getProjectionFromOpts($opts);

        if ($projection === []) {
            return;
        }

        /** @var FilterQuery $accumulator */
        i\type_check($query, FilterQuery::class);

        $query->project($projection);
    }
}
