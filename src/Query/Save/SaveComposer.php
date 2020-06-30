<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Save;

use Improved as i;
use Jasny\DB\Mongo\Query\WriteQuery;
use Jasny\DB\Query\ComposerInterface;

/**
 * Composer for MongoDB save query.
 *
 * @template TItem
 * @implements ComposerInterface<WriteQuery,TItem>
 */
class SaveComposer implements ComposerInterface
{
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
        foreach ($items as $index => $item) {
            $id = is_array($item) ? ($item['_id'] ?? null) : ($item->_id ?? null);

            $args = $id === null
                ? ['insertOne', $item]
                : ['replaceOne', ['_id' => $id], $item, ['upsert' => true]];

            $query->addIndexed($index, ...$args);
        }

        return [];
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
    }
}
