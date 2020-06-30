<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Writer;

use Jasny\DB\Map\MapInterface;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Result\Result;
use Jasny\DB\Result\ResultBuilder;
use Jasny\DB\Writer\WriteInterface;
use MongoDB\Collection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Fetch data from a MongoDB collection.
 */
class Writer implements WriteInterface
{
    protected Save $save;
    protected Update $update;
    protected Delete $delete;

    /**
     * Class constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        $logger = new NullLogger();

        $this->save = (new Save($map))->withLogging($logger);
        $map = $this->save->getMap(); // Use same map as save for update and delete

        $this->update = (new Update($map))->withLogging($logger);

        $this->delete = (new Delete($map))
            ->withComposer($this->update->getComposer())
            ->withLogging($logger);
    }


    /**
     * Get the mongodb collection the associated with the service.
     */
    public function getCollection(): Collection
    {
        if (!isset($this->collection)) {
            throw new \LogicException("This is a template service. "
                . "Create a copy that's linked to a MongoDB Collection with `forCollection()`");
        }

        // All 3 services must use the same collection, so could return the collection of any of them.
        return $this->save->getCollection();
    }

    /**
     * Alias of `getCollection()`.
     */
    final public function getStorage(): Collection
    {
        return $this->getCollection();
    }

    /**
     * Create a copy of this service, linked to the MongoDB collection.
     *
     * @param Collection $collection
     * @return static
     */
    public function forCollection(Collection $collection): self
    {
        $copy = clone $this;

        $copy->save = $this->save->forCollection($collection);
        $copy->update = $this->update->forCollection($collection);
        $copy->delete = $this->delete->forCollection($collection);

        return $this->copyIsSame($copy) ? $this : $copy;
    }


    /**
     * Get the field map.
     */
    public function getMap(): MapInterface
    {
        // All 3 services must use the same map, so could return the map of any of them.
        return $this->save->getMap();
    }

    /**
     * Get a copy with a different field map.
     *
     * @param MapInterface $map
     * @return static
     */
    public function withMap(MapInterface $map): self
    {
        $copy = clone $this;

        $copy->save = $this->save->withMap($map);
        $copy->update = $this->update->withMap($map);
        $copy->delete = $this->delete->withMap($map);

        return $this->copyIsSame($copy) ? $this : $copy;
    }


    /**
     * Get the query builder used by this service.
     */
    public function getComposer(): ComposerInterface
    {
        // Update and delete use the same collection, so could return the query builder of any of both.
        return $this->update->getComposer();
    }

    /**
     * Get a copy with a different query composer.
     *
     * @param ComposerInterface ...$composers
     * @return static
     */
    public function withComposer(ComposerInterface ...$composers): self
    {
        $composer = count($composers) === 1 ? reset($composers) : new Composer(...$composers);

        $copy = clone $this;

        $copy->update = $this->update->withComposer($composer);
        $copy->delete = $this->delete->withComposer($composer);

        return $this->copyIsSame($copy) ? $this : $copy;
    }

    /**
     * Get the query builder used by this service for save queries.
     */
    public function getSaveComposer(): ComposerInterface
    {
        return $this->save->getComposer();
    }

    /**
     * Get a copy with a different query builder for save .
     *
     * @param ComposerInterface $composer  Save query builder
     * @return static
     */
    public function withSaveComposer(ComposerInterface $composer): self
    {
        $copy = clone $this;
        $copy->save = $this->save->withComposer($composer);

        return $this->copyIsSame($copy) ? $this : $copy;
    }

    /**
     * Get the query builder used by this service for update queries.
     */
    public function getUpdateQueryBuilder(): ComposerInterface
    {
        return $this->update->getComposer();
    }

    /**
     * Get a copy with a different query builder for update.
     *
     * @param ComposerInterface $composer  Update query builder
     * @return static
     */
    public function withUpdateComposer(ComposerInterface $composer): self
    {
        $copy = clone $this;
        $copy->update = $this->update->withComposer($composer);

        return $this->copyIsSame($copy) ? $this : $copy;
    }


    /**
     * Get the result builder used by this service.
     */
    public function getResultBuilder(): ResultBuilder
    {
        // All 3 services must use the same result builder, so could return the builder of any of them.
        return $this->save->getResultBuilder();
    }

    /**
     * Get a copy with a different result builder.
     *
     * @param ResultBuilder $composer
     * @return static
     */
    public function withResultBuilder(ResultBuilder $composer): self
    {
        $copy = clone $this;

        $copy->save = $this->save->withResultBuilder($composer);
        $copy->update = $this->update->withResultBuilder($composer);
        $copy->delete = $this->delete->withResultBuilder($composer);

        return $this->copyIsSame($copy) ? $this : $copy;
    }


    /**
     * Enable (debug) logging.
     *
     * @return static
     */
    public function withLogging(LoggerInterface $logger): self
    {
        $copy = clone $this;

        $copy->save = $this->save->withLogging($logger);
        $copy->update = $this->update->withLogging($logger);
        $copy->delete = $this->delete->withLogging($logger);

        return $this->copyIsSame($copy) ? $this : $copy;
    }


    /**
     * Check if copy is the same as this object.
     */
    private function copyIsSame(self $copy): bool
    {
        return
            $this->save === $copy->save &&
            $this->update === $copy->update &&
            $this->delete === $copy->delete;
    }


    /**
     * @inheritDoc
     */
    public function save($item, array $opts = []): Result
    {
        return $this->save->save($item, $opts);
    }

    /**
     * @inheritDoc
     */
    public function saveAll(iterable $items, array $opts = []): Result
    {
        return $this->save->saveAll($items, $opts);
    }

    /**
     * @inheritDoc
     */
    public function update(array $filter, $instructions, array $opts = []): Result
    {
        return $this->update->update($filter, $instructions, $opts);
    }

    /**
     * @inheritDoc
     */
    public function delete(array $filter, array $opts = []): Result
    {
        return $this->delete->delete($filter, $opts);
    }
}
