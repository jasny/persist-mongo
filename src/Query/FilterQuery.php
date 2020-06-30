<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query;

use Improved\IteratorPipeline\Pipeline;

/**
 * Representation of a MongoDB query for filtering documents, modeled as aggregation pipeline.
 * This object is mutable, as it's uses as accumulator by the query builders.
 */
class FilterQuery
{
    /** @var array<int,array<string,mixed>> */
    protected array $stages = [];

    /** @var array<string,mixed> */
    protected array $options = [];

    /** Cached value for isAggregate() method. */
    protected bool $isAggregate;

    /** Cached value for getOptions() method. */
    protected array $determinedOptions;

    /**
     * @var array<string,mixed>
     * Cached value for getFilter method.
     */
    protected array $filter;

    /** Are stages optimized? */
    protected bool $isOptimized = true;


    /**
     * Is aggregation required to execute the query?
     *
     * {@internal Ignoring the order and combination of stages. Might be an issue.}}
     */
    public function isAggregate(): bool
    {
        $simpleOperations = ['$match', '$limit', '$sort', '$skip', '$project'];

        $this->isAggregate ??= Pipeline::with($this->stages)
            ->map(fn(array $stage) => key($stage))
            ->hasAny(fn(string $operator) => !in_array($operator, $simpleOperations, true));

        return $this->isAggregate;
    }


    /**
     * Set MongoDB specific query option.
     *
     * @param string $name
     * @param mixed $value
     */
    public function setOption(string $name, $value): void
    {
        $this->options[$name] = $value;
    }

    /**
     * Get MongoDB query options.
     *
     * @param bool $fromStages  Determine additional options from stages. Set this to `true` for when running a simple
     *   query that doesn't use aggregation pipeline.
     */
    public function getOptions(bool $fromStages = false): array
    {
        if (!$fromStages) {
            return $this->options;
        }

        if ($this->isAggregate()) {
            throw new \BadMethodCallException("Unable to convert stages to options for an aggregate pipeline");
        }

        $map = ['$limit' => 'limit', '$sort' => 'sort', '$skip' => 'skip', '$project' => 'projection'];

        $this->determinedOptions ??= Pipeline::with($this->stages)
            ->flatten(true)
            ->mapKeys(fn($_, string $operator) => $map[$operator] ?? null)
            ->cleanup()
            ->toArray();

        return $this->determinedOptions + $this->options;
    }


    /**
     * Add a statement to the query.
     *
     * @param string $operation
     * @param mixed  $statement
     */
    public function addStage(string $operation, $statement): void
    {
        $this->stages[] = [$operation => $statement];

        // Needs to be reevaluated.
        $this->isOptimized = false;
        unset($this->isAggregate, $this->determinedOptions, $this->filter);
    }

    /**
     * Adds new fields to documents. $addFields outputs documents that contain all existing fields from the input
     * documents and newly added fields.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
     *
     * @param array<string,mixed> $specification
     */
    public function addFields(array $specification): void
    {
        $this->addStage('$addFields', $specification);
    }

    /**
     * Passes a document to the next stage that contains a count of the number of documents input to the stage.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/count/
     */
    public function count(string $field = 'count'): void
    {
        $this->addStage('$count', $field);
    }

    /**
     * Groups input documents by the specified _id expression and for each distinct grouping, outputs a document.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/group/
     *
     * @param array<string,mixed> $specification
     */
    public function group(array $specification): void
    {
        $this->addStage('$group', $specification);
    }

    /**
     * Limits the number of documents passed.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/limit/
     */
    public function limit(int $count): void
    {
        $this->addStage('$limit', $count);
    }

    /**
     * Performs a left outer join to an unsharded collection in the same database to filter in documents from the
     * “joined” collection for processing.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/
     */
    public function lookup(string $from, string $localField, string $foreignField, string $as)
    {
        $this->addStage('$lookup', compact('from', 'localField', 'foreignField', 'as'));
    }

    /**
     * Filters the documents to pass only the documents that match the specified condition(s) to the next pipeline
     * stage.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/match/
     *
     * @param array<string,mixed> $query
     */
    public function match(array $query): void
    {
        $this->addStage('$match', $query);
    }

    /**
     * Passes along the documents with the requested fields to the next stage in the pipeline.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/project/
     *
     * @param array<string,mixed> $specification
     */
    public function project(array $specification): void
    {
        $this->addStage('$project', $specification);
    }

    /**
     * Limits the number of documents passed.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/limit/
     */
    public function skip(int $count): void
    {
        $this->addStage('$skip', $count);
    }

    /**
     * Passes along the documents with the requested fields to the next stage in the pipeline.
     * @see https://docs.mongodb.com/manual/reference/operator/aggregation/project/
     *
     * @param array<string,int> $specification
     */
    public function sort(array $specification): void
    {
        $this->addStage('$sort', $specification);
    }


    /**
     * Get the query operators for a simple query like `find()`.
     *
     * @return array<string,mixed>
     * @throws \LogicException if the query requires an aggregate pipeline
     */
    public function getFilter(): array
    {
        if ($this->isAggregate()) {
            throw new \LogicException("Unable to get the filter; the query requires an aggregate pipeline");
        }

        if (!isset($this->filter)) {
            $matchStages = Pipeline::with($this->stages)
                ->filter(fn(array $stage) => key($stage) === '$match')
                ->map(fn(array $stage) => reset($stage))
                ->toArray();

            $this->filter = $this->reduceMatch(...$matchStages);
        }

        return $this->filter;
    }

    /**
     * Get all pipeline stages.
     *
     * @return array<array<string,mixed>>
     */
    public function getPipeline(): array
    {
        if (!$this->isOptimized) {
            $this->optimizeStages();
        }

        return $this->stages;
    }

    /**
     * Combine sequential match stages.
     */
    protected function optimizeStages(): void
    {
        $optimized = [];
        $matchStages = [];

        foreach ($this->stages as $stage) {
            if (key($stage) === '$match') {
                $matchStages[] = $stage;
                continue;
            }

            if ($matchStages !== []) {
                $optimized[] = $this->reduceMatch(...$matchStages);
                $matchStages = [];
            }
            $optimized[] = $stage;
        }

        if ($matchStages !== []) {
            $optimized[] = $this->reduceMatch(...$matchStages);
        }

        $this->stages = $optimized;
        $this->isOptimized = true;
    }

    /**
     * Combine statements from match stages into a single match statement.
     *
     * @param array<string,mixed> ...$statements
     * @return array<string,mixed>
     */
    protected function reduceMatch(array ...$statements): array
    {
        if (count($statements) === 1) {
            return $statements[0];
        }

        $hasOr = Pipeline::with($statements)
            ->flatten(true)
            ->hasAny(fn($_, string $key) => $key[0] === '$' && $key !== '$and');

        if ($hasOr) {
            return ['$and' => $statements];
        }

        return Pipeline::with($statements)
            ->flatten(true)
            ->group(fn($_, string $key) => $key)
            ->map(fn(array $value, string $key) => ($key === '$and' ? array_merge(...$value) : $value))
            ->toArray();
    }
}
