<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Update;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Exception\UnsupportedFeatureException;
use Jasny\DB\Option\LimitOption;
use Jasny\DB\Query\ComposerInterface;

/**
 * Assert that query doesn't have a limit option or has a limit of 1.
 *
 * @implements ComposerInterface<object,mixed>
 */
class OneOrMany implements ComposerInterface
{
    /**
     * @inheritDoc
     */
    public function compose(object $query, iterable $items, array $opts = []): void
    {
        throw new \LogicException(__CLASS__ . ' can only be used in combination with other query composers');
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
        /** @var LimitOption|null $limit */
        $limit = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof LimitOption)
            ->last();

        if (isset($limit) && $limit->getLimit() !== 1) {
            $msg = "MongoDB write can be on one document or all documents, but not exactly " . $limit->getLimit();
            throw new UnsupportedFeatureException($msg);
        }

        if (isset($limit) && $limit->getOffset() !== 0) {
            $msg = "MongoDB write can't be done based on an offset";
            throw new UnsupportedFeatureException($msg);
        }
    }
}
