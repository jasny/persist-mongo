<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo;

use Improved as i;
use Improved\IteratorPipeline\Pipeline;
use Jasny\DB\Map\MapInterface;
use Jasny\DB\Map\NoMap;
use Jasny\DB\Mongo\Model\BSONToPHP;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Option\OptionInterface;
use Jasny\DB\Option\SettingOption;
use Jasny\DB\Query\Composer;
use Jasny\DB\Query\ComposerInterface;
use Jasny\DB\Result\ResultBuilder;
use Jasny\DB\Schema\Schema;
use Jasny\DB\Schema\SchemaInterface;
use Jasny\Immutable;
use MongoDB\Database;
use MongoDB\Collection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Abstract base class for reader and writer.
 * @internal
 *
 * @template TQuery
 * @template TQueryItem
 */
abstract class AbstractService
{
    use Immutable\With;

    protected Database $database;
    protected Collection $collection;

    protected SchemaInterface $schema;
    protected ?MapInterface $map = null;

    /** @phpstan-var ComposerInterface<TQuery,TQueryItem> */
    protected ComposerInterface $composer;
    protected ResultBuilder $resultBuilder;

    protected LoggerInterface $logger;

    /**
     * Reader constructor.
     *
     * @param Database|Collection|null $storage
     */
    public function __construct($storage)
    {
        i\type_check($storage, [Database::class, Collection::class, 'null']);

        if ($storage instanceof Database) {
            /** @var Database $storage */
            $this->database = $storage;
        }
        if ($storage instanceof Collection) {
            /** @var Collection $storage */
            $this->collection = $storage;
        }

        $this->schema = new Schema();
        $this->resultBuilder = (new ResultBuilder())->map(new BSONToPHP());
        $this->logger = new NullLogger();
    }


    /**
     * Get the mongodb collection the associated with the service.
     */
    public function getCollection(): Collection
    {
        if (!isset($this->collection)) {
            throw new \LogicException(
                "Create a copy that's linked to a MongoDB Collection by calling the `for()` method"
            );
        }

        return $this->collection;
    }

    /**
     * Get the MongoDB collection or database used by the reader.
     *
     * @return Database|Collection
     */
    public function getStorage()
    {
        if (!isset($this->collection) && !isset($this->database)) {
            throw new \LogicException(
                "Create a copy that's linked to a MongoDB Collection or Database by calling the `for()` method"
            );
        }

        return $this->collection ?? $this->database;
    }

    /**
     * Create a copy of this service, linked to a MongoDB collection or database.
     *
     *     $reader = new Reader($db);
     *     $fooReader = $reader->for('foo');    // Collection name
     *     $barReader = $reader->for($db->bar); // MongoDB\Collection object
     *
     * @param Database|Collection|string $storage
     * @return static
     */
    public function for($storage): self
    {
        return $storage instanceof Database
            ?  $this->forDatabase($storage)
            : $this->forCollection($storage);
    }

    /**
     * Create a copy of this service linked to a MongoDB database.
     *
     * @param Database $database
     * @return static
     */
    private function forDatabase(Database $database): self
    {
        return $this
            ->withoutProperty('collection')
            ->withProperty('database', $database);
    }

    /**
     * Create a copy of this service linked to a MongoDB collection.
     *
     * @param Collection|string $collection
     * @return static
     */
    private function forCollection($collection): self
    {
        $copy = $this;

        if (!$collection instanceof Collection) {
            if (!isset($this->database)) {
                throw new \LogicException("Not connected to a MongoDB Database object");
            }

            $collection = $this->database->selectCollection($collection);
        } elseif (!$this->usesSameDatabase($collection)) {
            $copy = $copy->withoutProperty('database');
        }

        return $copy->withProperty('collection', $collection);
    }

    /**
     * Check if the give Collection uses the same MongoDB database.
     *
     * @param Collection $collection
     * @return bool
     */
    private function usesSameDatabase(Collection $collection): bool
    {
        return
            isset($this->database) &&
            $this->database->getManager() === $collection->getManager() &&
            $this->database->getDatabaseName() === $collection->getDatabaseName();
    }


    /**
     * Get the database schema.
     */
    public function getSchema(): SchemaInterface
    {
        return $this->schema;
    }

    /**
     * Get a copy with a different field map.
     *
     * @param SchemaInterface $schema
     * @return static
     */
    public function withSchema(SchemaInterface $schema)
    {
        return $this->withProperty('schema', $schema);
    }

    /**
     * Get the field map.
     */
    public function getMap(): MapInterface
    {
        return $this->map ??
            (isset($this->collection) ? $this->schema->map($this->collection->getCollectionName()) : new NoMap());
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
     * Configure opts, inserting map and schema.
     *
     * @param OptionInterface[] $opts
     */
    protected function configure(array &$opts): void
    {
        $this->configureMap($opts);

        array_unshift(
            $opts,
            opts\setting('schema', $this->schema),
            opts\setting('collection', $this->collection->getCollectionName()),
        );
    }

    /**
     * Configure map in opts.
     *
     * @param OptionInterface[] $opts
     */
    private function configureMap(array &$opts): void
    {
        $map = opts\setting('map', $this->getMap())->findIn($opts, MapInterface::class);

        $opts = Pipeline::with($opts)
            ->filter(fn($opt) => !($opt instanceof SettingOption) || $opt->getName() !== 'map')
            ->toArray();

        $opts[] = opts\setting('map', $map->withOpts($opts));
    }


    /**
     * Get the query composer used by this service.
     */
    public function getComposer(): ComposerInterface
    {
        return $this->composer;
    }

    /**
     * Get a copy with a different query composer.
     * If multiple composers are given, they're combined.
     *
     * @param ComposerInterface ...$composers
     * @return static
     */
    public function withComposer(ComposerInterface ...$composers)
    {
        return $this->withProperty(
            'composer',
            count($composers) === 1 ? $composers[0] : new Composer(...$composers)
        );
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
