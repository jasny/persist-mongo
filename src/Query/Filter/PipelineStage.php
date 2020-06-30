<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Query\ComposerInterface;

/**
 * Add a new stage to the aggregation pipeline.
 * The stage is added in the 'in the 'apply' step.
 *
 * @implements ComposerInterface<FilterQuery,mixed>
 */
class PipelineStage implements ComposerInterface
{
    protected string $operation;

    /** @var mixed */
    protected $statement;

    /**
     * @param string $operation
     * @param mixed  $statement
     */
    public function __construct(string $operation, $statement)
    {
        $this->operation = $operation;
        $this->statement = $statement;
    }

    /**
     * @inheritDoc
     */
    public function compose(object $query, iterable $items, array $opts = []): void
    {
        i\iterable_walk(
            $this->apply($query, $items, $opts)
        );
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
        /** @var FilterQuery $query */
        i\type_check($query, FilterQuery::class);

        $query->addStage($this->operation, $this->statement);

        return $items;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
    }
}
