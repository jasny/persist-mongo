<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo;

use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\FieldMap;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Mongo\Model\BSONToPHP;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Option\SettingOption;
use Jasny\DB\QueryBuilder\QueryBuilderInterface;
use Jasny\DB\Result\ResultBuilder;
use Jasny\Immutable;
use MongoDB\Collection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Abstract base class for reader and writer.
 */
abstract class AbstractService
{
    use Immutable\With;

    protected Collection $collection;
    protected MapInterface $map;

    protected QueryBuilderInterface $queryBuilder;
    protected ResultBuilder $resultBuilder;

    protected LoggerInterface $logger;

    /**
     * Reader constructor.
     */
    public function __construct(?MapInterface $map = null)
    {
        $this->map = $map ?? new FieldMap(['id' => '_id']);

        $this->resultBuilder = (new ResultBuilder())->map(new BSONToPHP());
        $this->logger = new NullLogger();
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

        return $this->collection;
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
    public function forCollection(Collection $collection)
    {
        return $this->withProperty('collection', $collection);
    }


    /**
     * Get the field map.
     */
    public function getMap(): MapInterface
    {
        return $this->map;
    }

    /**
     * Get a copy with a different field map.
     *
     * @param MapInterface $map
     * @return static
     */
    public function withMap(MapInterface $map)
    {
        return $this->withProperty('map', $map);
    }

    /**
     * Configure the map in opts.
     *
     * @param OptionInterface[] $opts
     */
    protected function configureMap(array &$opts): void
    {
        $map = opts\setting('map', $this->map)->findIn($opts, MapInterface::class);

        $opts = Pipeline::with($opts)
            ->filter(fn($opt) => !($opt instanceof SettingOption) || $opt->getName() !== 'map')
            ->toArray();

        $opts[] = opts\setting('map', $map->withOpts($opts));
    }


    /**
     * Get the query builder used by this service.
     */
    public function getQueryBuilder(): QueryBuilderInterface
    {
        return $this->queryBuilder;
    }

    /**
     * Get a copy with a different query builder.
     *
     * @param QueryBuilderInterface $builder  Save query builder
     * @return static
     */
    public function withQueryBuilder(QueryBuilderInterface $builder)
    {
        return $this->withProperty('queryBuilder', $builder);
    }


    /**
     * Get the result builder used by this service.
     */
    public function getResultBuilder(): ResultBuilder
    {
        return $this->resultBuilder;
    }

    /**
     * Get a copy with a different result builder.
     *
     * @param ResultBuilder $builder
     * @return static
     */
    public function withResultBuilder(ResultBuilder $builder)
    {
        return $this->withProperty('resultBuilder', $builder);
    }


    /**
     * Enable (debug) logging.
     *
     * @return static
     */
    public function withLogging(LoggerInterface $logger)
    {
        return $this->withProperty('logger', $logger);
    }

    /**
     * Log a debug message.
     *
     * @param string       $message
     * @param array<mixed> $context
     */
    protected function debug(string $message, array $context): void
    {
        $this->logger->debug(sprintf($message, $this->getCollection()->getCollectionName()), $context);
    }
}
