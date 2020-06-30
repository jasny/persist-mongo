<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Map;

use MongoDB\BSON;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\Traits\ProxyTrait;

/**
 * Wrapper for map to asserting that no illegal field names are used.
 * Field names should not be empty, contain a `.`, or start with with a `$`.
 */
class AssertMap implements MapInterface
{
    use ProxyTrait;

    /**
     * Class constructor.
     *
     * @param MapInterface $map
     */
    public function __construct(MapInterface $map)
    {
        $this->inner = $map;
    }

    /**
     * @inheritDoc
     * @throws \UnexpectedValueException if field name is illegal
     */
    public function applyToField(string $field)
    {
        if ($field[0] === '$' || strpos($field, '.$') !== false) {
            throw new \UnexpectedValueException("Illegal field name '$field': starting with '$' isn't allowed");
        }

        return $this->inner->applyToField($field);
    }


    /**
     * @inheritDoc
     */
    public function apply($item)
    {
        $this->assertItem($item);

        return $this->inner->apply($item);
    }

    /**
     * Ensure the value has no keys that are mongo operators (recursively).
     *
     * @param iterable|object $element
     * @param string[]        $parents
     * @throws \UnexpectedValueException
     */
    protected function assertItem($element, array $parents = []): void
    {
        if (count($parents) >= 32) {
            throw new \OverflowException("Possible circular reference");
        }

        foreach ($element as $key => $value) {
            if (is_string($key) && ($key[0] === '$' || strpos($key, '.') !== false)) {
                $desc = "'{$key}'" . ($parents !== [] ? " in '" . join('.', $parents) . "'" : '');
                $reason = $key[0] === '$' ? "starting with '$' isn't allowed" : "may not contain a '.'";
                throw new \UnexpectedValueException("Illegal field name {$desc}: {$reason}");
            }

            if (is_array($value) || (is_object($value) && !$value instanceof BSON\Type)) {
                $this->assertItem($value, array_merge($parents, [$key])); // recursion
            }
        }
    }
}
