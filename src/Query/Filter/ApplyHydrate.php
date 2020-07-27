<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Mongo\Query\FilterQuery;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\HydrateOption;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Schema\Relationship;
use Jasny\DB\Schema\SchemaInterface;
use function Jasny\DB\Mongo\extract_opts;

/**
 * Apply a hydrate option to the query as $lookup pipeline stage.
 */
class ApplyHydrate implements ComposerInterface
{
    use ProjectionTrait;
    use LookupTrait;


    /**
     * Get 'collection' setting from opts.
     *
     * @param OptionInterface[] $opts
     * @return string
     */
    protected function getCollectionSetting(array $opts): string
    {
        $collection = opts\setting('collection', null)->findIn($opts);

        if ($collection === null) {
            throw new \LogicException("Failed to apply hydrate option; missing 'collection' setting");
        }

        return $collection;
    }

    /**
     * Get 'collection' setting from opts.
     *
     * @param OptionInterface[] $opts
     * @return SchemaInterface
     */
    protected function getSchemaSetting(array $opts): SchemaInterface
    {
        $schema = opts\setting('schema', null)->findIn($opts, SchemaInterface::class);

        if ($schema === null) {
            throw new \LogicException("Failed to apply hydrate option; no schema configured");
        }

        return $schema;
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
    public function finalize(object $query, array $opts): void
    {
        return;
    }


    /**
     * @inheritDoc
     */
    public function apply(object $query, iterable $items, array $opts): iterable
    {
        /** @var HydrateOption[] $opts */
        $hydrateOpts = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof HydrateOption)
            ->toArray();

        // Quick return
        if ($hydrateOpts === []) {
            return $items;
        }

        /** @var FilterQuery $query */
        i\type_check($query, FilterQuery::class);

        $collection = $this->getCollectionSetting($opts);
        $schema = $this->getSchemaSetting($opts);

        foreach ($hydrateOpts as $opt) {
            $this->applyOpt($query, $opt, $collection, $schema);
        }

        return $items;
    }

    /**
     * Apply a hydrate option to the query.
     */
    protected function applyOpt(
        FilterQuery $query,
        HydrateOption $opt,
        string $collection,
        SchemaInterface $schema
    ): void {
        $relationship = $schema->getRelationshipForField($collection, $opt->getField());

        $name = $opt->getName();
        $subOpts = $opt->getOpts();

        if (count($relationship->getMatch()) !== 1 || $subOpts !== []) {
            $this->addComplexLookup($query, $relationship, $name, $subOpts);
        } else {
            $this->addSimpleLookup($query, $relationship, $name);
        }

        if ($opt->getName() !== $opt->getField()) {
            $query->addStage('$unset', [$opt->getField()]);
        }
    }

    /**
     * Add a simple lookup stage (matching on single field) to the query.
     *
     * @param FilterQuery  $query
     * @param Relationship $relationship
     * @param string       $name
     */
    protected function addSimpleLookup(FilterQuery $query, Relationship $relationship, string $name): void
    {
        $match = $relationship->getMatch();
        $query->lookup($relationship->getRelatedCollection(), key($match), reset($match), $name);

        if (!$relationship->isToMany()) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name, 0]]]);
        }
    }

    /**
     * Add complex lookup stage with pipeline to the query.
     *
     * @param FilterQuery       $query
     * @param Relationship      $relationship
     * @param string            $name
     * @param OptionInterface[] $opts
     */
    private function addComplexLookup(
        FilterQuery $query,
        Relationship $relationship,
        string $name,
        array $opts
    ): void {
        $match = $relationship->getMatch();
        $let = $this->calcLookupLet($match);

        $pipeline = [];
        $pipeline[] = ['$match' => ['$expr' => $this->calcMatchExpr($match)]];
        $this->addProjectStage($pipeline, $opts);

        $lookup = [
            'from' => $relationship->getRelatedCollection(),
            'let' => $let,
            'pipeline' => $pipeline,
            'as' => $name,
        ];

        $query->addStage('$lookup', $lookup);

        if (!$relationship->isToMany()) {
            $query->addFields([$name => ['$arrayElemAt' => ['$' . $name, 0]]]);
        }
    }

    /**
     * @param array<int,array<string,mixed>> $pipeline
     * @param OptionInterface[]              $opts
     */
    private function addProjectStage(array &$pipeline, array $opts): void
    {
        $projection = $this->getProjectionFromOpts($opts);

        if ($projection !== []) {
            $pipeline[] = ['$project' => $projection];
        }
    }
}
