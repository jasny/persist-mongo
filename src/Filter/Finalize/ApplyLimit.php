<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Filter\Finalize;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Mongo\Query\QueryInterface;
use Jasny\DB\Option\LimitOption;
use Jasny\DB\Option\OptionInterface;

/**
 * Convert limit query option to a MongoDB query.
 * If multiple limit options are present, only the last is used.
 */
class ApplyLimit
{
    /**
     * Apply limit opt to a MongoDB query.
     *
     * @param QueryInterface    $query
     * @param OptionInterface[] $opts
     */
    public function __invoke(QueryInterface $query, array $opts): void
    {
        /** @var LimitOption|null $limitOpt */
        $limitOpt = Pipeline::with($opts)
            ->filter(fn($opt) => $opt instanceof LimitOption)
            ->last();

        if ($limitOpt === null) {
            return;
        }

        $query->setOption('limit', $limitOpt->getLimit());

        if ($limitOpt->getOffset() !== 0) {
            $query->setOption('skip', $limitOpt->getOffset());
        }
    }
}
