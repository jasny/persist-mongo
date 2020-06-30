<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Query\Update;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Mongo\Query\UpdateQuery;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Update\UpdateInstruction;
use function Jasny\DB\Mongo\flatten_fields;

/**
 * Standard compose step for update query.
 *
 * @implements ComposerInterface<UpdateQuery,UpdateInstruction>
 */
class UpdateComposer implements ComposerInterface
{
    /**
     * @inheritDoc
     */
    public function compose(object $query, iterable $instructions, array $opts = []): void
    {
        $this->apply($query, $instructions, $opts);
    }

    /**
     * @inheritDoc
     */
    public function prepare(iterable $instructions, array &$opts = []): iterable
    {
        return $instructions;
    }

    /**
     * @inheritDoc
     */
    public function apply(object $query, iterable $instructions, array $opts): iterable
    {
        foreach ($instructions as $instruction) {
            $operation = $this->getOperation($instruction->getOperator(), $instruction->getPairs());

            if ($operation === null) {
                yield $operation;
                continue;
            }

            $query->add($operation);
        }
    }

    /**
     * Get the query operation.
     *
     * @param string $operator
     * @param array  $pairs
     * @return array|null
     */
    protected function getOperation(string $operator, array $pairs): ?array
    {
        $numPairs = in_array($operator, ['inc', 'mul', 'div'])
            ? Pipeline::with($pairs)->typeCheck(['int', 'float'], new \UnexpectedValueException())
            : null;

        switch ($operator) {
            case 'set':
                return ['$set' => $pairs];
            case 'patch':
                return ['$set' => flatten_fields($pairs)];
            case 'inc':
                return ['$inc' => $numPairs->toArray()];
            case 'mul':
                return ['$mul' => $numPairs->toArray()];
            case 'div':
                return ['$mul' => $numPairs->map(fn($value) => 1 / (float)$value)->toArray()];
            case 'push':
                return ['$push' => Pipeline::with($pairs)->map(fn($value) => ['$each' => $value])->toArray()];
            case 'pull':
                return ['$pullAll' => $pairs];
        }

        return null;
    }

    /**
     * @inheritDoc
     */
    public function finalize(object $query, array $opts): void
    {
    }
}
