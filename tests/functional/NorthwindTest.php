<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo\Tests\Functional;

use Improved as i;
use Jasny\DB\Mongo\Reader\Reader;
use Jasny\DB\Mongo\Writer\Writer;
use Jasny\DB\Option\Functions as opts;
use Jasny\DB\Schema\Schema;
use MongoDB\Client;
use MongoDB\Database;
use Monolog\Formatter\JsonFormatter;
use Monolog\Handler\NullHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

/**
 * Test against the Northwind database.
 * Northwind has a flat structure as it is exported from an SQL database.
 *
 * @see https://github.com/tmcnab/northwind-mongo
 * @see https://docs.yugabyte.com/latest/sample-data/northwind/
 */
class NorthwindTest extends TestCase
{
    protected Database $db;
    protected Logger $logger;
    protected Schema $schema;
    protected Reader $reader;

    public function setUp(): void
    {
        $client = new Client();
        if (i\iterable_count($client->listDatabases(['filter' => ['name' => 'Northwind']])) === 0) {
            $this->markTestSkipped("The 'Northwind' database doesn't exist");
        }

        $typeMap = ['array' => 'array', 'document' => 'array', 'root' => 'array'];
        $this->db = $client->selectDatabase('Northwind', ['typeMap' => $typeMap]);

        $this->setUpLogger();

        $this->schema = (new Schema())
            ->withOneToMany('customers', 'orders', ['CustomerID' => 'CustomerID'])
            ->withOneToMany('orders', 'order_details', ['OrderID' => 'OrderID'])
            ->withManyToOne('order_details', 'products', ['ProductID' => 'ProductID'])
            ->withManyToOne('products', 'categories', ['CategoryID' => 'CategoryID'])
            ->withManyToOne('products', 'suppliers', ['SupplierID' => 'SupplierID'])
            ->withOneToMany('employees', 'orders', ['EmployeeID' => 'EmployeeID']);

        $this->reader = (new Reader($this->db))
            ->withSchema($this->schema)
            ->withLogging($this->logger);
    }

    private function setUpLogger(): void
    {
        if (getenv('JASNY_DB_TESTS_DEBUG') === 'on') {
            $formatter = (new JsonFormatter())->setMaxNormalizeDepth(100);
            $handler = (new StreamHandler(STDERR))->setFormatter($formatter);
            $this->logger = new Logger('MongoDB', [$handler]);
        } else {
            $this->logger = new Logger('MongoDB', [new NullHandler()]);
        }
    }

    public function testFetchOrderWithCustomer()
    {
        $reader = $this->reader->for('orders');

        $order = $reader
            ->fetch(
                ["OrderID" => 10248],
                opts\limit(1),
                opts\hydrate('CustomerID')->omit('_id'),
                opts\hydrate('EmployeeID')->fields('EmployeeID', 'LastName', 'FirstName', 'Title'),
                opts\omit('_id'),
            )
            ->first(true);

        $this->assertIsArray($order);

        $this->assertArrayHasKey('Customer', $order);
        $this->assertArrayNotHasKey('CustomerID', $order);

        $this->assertArrayHasKey('Employee', $order);
        $this->assertArrayNotHasKey('EmployeeID', $order);

        $expected = [
            'OrderID' => 10248,
            'OrderDate' => '1996-07-04 00:00:00.000',
            'RequiredDate' => '1996-08-01 00:00:00.000',
            'ShippedDate' => '1996-07-16 00:00:00.000',
            'ShipVia' => 3,
            'Freight' => 32.38,
            'ShipName' => 'Vins et alcools Chevalier',
            'ShipAddress' => '59 rue de l\'Abbaye',
            'ShipCity' => 'Reims',
            'ShipRegion' => 'NULL',
            'ShipPostalCode' => 51100,
            'ShipCountry' => 'France',
            'Customer' => [
                'CustomerID' => 'VINET',
                'CompanyName' => 'Vins et alcools Chevalier',
                'ContactName' => 'Paul Henriot',
                'ContactTitle' => 'Accounting Manager',
                'Address' => '59 rue de l\'Abbaye',
                'City' => 'Reims',
                'Region' => 'NULL',
                'PostalCode' => 51100,
                'Country' => 'France',
                'Phone' => '26.47.15.10',
                'Fax' => '26.47.15.11',
            ],
            'Employee' => [
                'EmployeeID' => 5,
                'LastName' => 'Buchanan',
                'FirstName' => 'Steven',
                'Title' => 'Sales Manager',
            ],
        ];

        $this->assertEquals($expected, $order);
    }

    public function testFetchCustomerWithOrders()
    {
        $reader = $this->reader->for('customers');

        $customer = $reader
            ->fetch(
                ["CustomerID" => "VINET"],
                opts\limit(1),
                opts\lookup('orders')->limit(3)->fields('OrderID', 'OrderDate')->as('Orders'),
                opts\omit('_id')
            )
            ->first(true);

        $this->assertIsArray($customer);
        $this->assertArrayHasKey('Orders', $customer);

        $expected = [
            'CustomerID' => 'VINET',
            'CompanyName' => 'Vins et alcools Chevalier',
            'ContactName' => 'Paul Henriot',
            'ContactTitle' => 'Accounting Manager',
            'Address' => '59 rue de l\'Abbaye',
            'City' => 'Reims',
            'Region' => 'NULL',
            'PostalCode' => 51100,
            'Country' => 'France',
            'Phone' => '26.47.15.10',
            'Fax' => '26.47.15.11',
            'Orders' => [
                [
                    'OrderID' => 10248,
                    'OrderDate' => '1996-07-04 00:00:00.000',
                ],
                [
                    'OrderID' => 10274,
                    'OrderDate' => '1996-08-06 00:00:00.000',
                ],
                [
                    'OrderID' => 10295,
                    'OrderDate' => '1996-09-02 00:00:00.000',
                ],
            ],
        ];

        $this->assertEquals($expected, $customer);
    }

    public function testFetchCustomerWithOrderCount()
    {
        $reader = $this->reader->for('customers');

        $customer = $reader
            ->fetch(
                ["CustomerID" => "VINET"],
                opts\limit(1),
                opts\lookup('orders')->count()->as('Orders'),
                opts\fields('CustomerID', 'CompanyName', 'Orders')
            )
            ->first(true);

        $this->assertIsArray($customer);
        $this->assertArrayHasKey('Orders', $customer);

        $expected = [
            'CustomerID' => 'VINET',
            'CompanyName' => 'Vins et alcools Chevalier',
            'Orders' => 5
        ];

        $this->assertEquals($expected, $customer);
    }
}
