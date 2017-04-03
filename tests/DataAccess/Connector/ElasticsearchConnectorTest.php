<?php

namespace Honeybee\Tests\Elasticsearch2\DataAccess\Connector;

use Elasticsearch\Client;
use Honeybee\Elasticsearch2\DataAccess\Connector\ElasticsearchConnector;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\ConfigInterface;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterfaceTest;
use Honeybee\Infrastructure\DataAccess\Connector\Status;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;

class ElasticsearchConnectorTest extends TestCase
{
    protected function getConnector($name, ConfigInterface $config)
    {
        return new ElasticsearchConnector($name, $config);
    }

    public function testGetNameWorks()
    {
        $connector = $this->getConnector('conn1', new ArrayConfig(['name' => 'foo']));
        $this->assertSame('conn1', $connector->getName());
    }

    public function testGetConnectionWorks()
    {
        $connector = $this->getConnector('conn1', new ArrayConfig([]));
        $connection = $connector->getConnection();
        $this->assertTrue($connector->isConnected(), 'Connector should be connected after getConnection() call');
        $this->assertTrue(is_object($connection), 'A getConnection() call should yield a client/connection object');
    }

    public function testDisconnectWorks()
    {
        $connector = $this->getConnector('conn1', new ArrayConfig([]));
        $connector->getConnection();
        $this->assertTrue($connector->isConnected());
        $connector->disconnect();
        $this->assertFalse($connector->isConnected());
    }

    public function testGetConfigWorks()
    {
        $connector = $this->getConnector('conn1', new ArrayConfig(['foo' => 'bar']));
        $this->assertInstanceOf(ConfigInterface::CLASS, $connector->getConfig());
        $this->assertSame('bar', $connector->getConfig()->get('foo'));
    }

    public function testFakingStatusAsFailingSucceeds()
    {
        $connector = $this->getConnector('failing', new ArrayConfig(['fake_status' => Status::FAILING]));
        $status = $connector->getStatus();
        $this->assertTrue($status->isFailing());
        $this->assertSame('failing', $status->getConnectionName());
    }

    public function testFakingStatusAsWorkingSucceeds()
    {
        $connector = $this->getConnector('working', new ArrayConfig(['fake_status' => Status::WORKING]));
        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertSame('working', $status->getConnectionName());
    }

    public function testPingEndpointIsCalledOnStatus()
    {
        $mock_client = Mockery::mock(Client::CLASS);
        $mock_client->shouldReceive('ping')->once()->withNoArgs()->andReturn(true);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig([])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mock_client);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['message' => 'Pinging elasticsearch succeeded.'], $status->getDetails());
    }
}
