<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Filter;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Schema\SchemaInterface;

/**
 * Methods to add a MongoDB '$lookup' stage based on a Relationship.
 */
trait LookupTrait
{
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
            $type = strtolower(str_replace('Apply', '', get_class($this)));
            throw new \LogicException("Failed to apply $type option; missing 'collection' setting");
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
            $type = strtolower(str_replace('Apply', '', get_class($this)));
            throw new \LogicException("Failed to apply $type option; no schema configured");
        }

        return $schema;
    }

    /**
     * Calculate the `let` property for a complex lookup.
     *
     * @param array<string,string> $match
     * @return array<string,string>
     */
    private function calcLookupLet(array $match): array
    {
        return Pipeline::with($match)
            ->map(fn($_, $field) => '$' . $field)
            ->mapKeys(fn($_, $field) => strtolower($field))
            ->toArray();
    }


    /**
     * Calculate the match expression for the lookup pipeline.
     *
     * @param array<string,string> $match
     */
    private function calcMatchExpr(array $match): array
    {
        $expr = Pipeline::with($match)
            ->map(fn($var) => '$$' . strtolower($var))
            ->map(fn($var, $field) => ['$eq' => ['$' . $field, $var]])
            ->values()
            ->toArray();

        return count($expr) === 1 ? $expr[0] : ['$and' => $expr];
    }
}
