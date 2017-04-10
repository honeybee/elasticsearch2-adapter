<?php

namespace Honeybee\Tests\Elasticsearch2\Connector;

use Elasticsearch\Client;
use Honeybee\Elasticsearch2\Connector\ElasticsearchConnector;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\ConfigInterface;
use Honeybee\Infrastructure\DataAccess\Connector\Status;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;

class ElasticsearchConnectorTest extends TestCase
{
    private function getConnector($name, ConfigInterface $config)
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
        $this->assertSame($connection, $connector->getConnection());
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

    public function testGetStatusPing()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('ping')->once()->withNoArgs()->andReturn(true);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig([])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['message' => 'Pinging Elasticsearch succeeded.'], $status->getDetails());
    }

    public function testGetStatusPingFailing()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('ping')->once()->withNoArgs()->andReturn(false);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig([])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertFalse($status->isWorking());
        $this->assertEquals(['message' => 'Pinging Elasticsearch failed.'], $status->getDetails());
    }

    public function testGetStatusInfo()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('info')->once()->withNoArgs()->andReturn(['status' => 'info']);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig(['status_test' => 'info'])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['status' => 'info'], $status->getDetails());
    }

    public function testGetStatusClusterHealth()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('cluster')->once()->withNoArgs()->andReturnSelf();
        $mockClient->shouldReceive('health')->once()->withNoArgs()->andReturn(['status' => 'health']);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig(['status_test' => 'cluster_health'])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['status' => 'health'], $status->getDetails());
    }

    public function testGetStatusClusterStats()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('cluster')->once()->withNoArgs()->andReturnSelf();
        $mockClient->shouldReceive('stats')->once()->withNoArgs()->andReturn(['status' => 'stats']);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig(['status_test' => 'cluster_stats'])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['status' => 'stats'], $status->getDetails());
    }

    public function testGetStatusNodesStats()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('nodes')->once()->withNoArgs()->andReturnSelf();
        $mockClient->shouldReceive('stats')->once()->withNoArgs()->andReturn(['status' => 'nodes']);

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig(['status_test' => 'nodes_stats'])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertTrue($status->isWorking());
        $this->assertEquals(['status' => 'nodes'], $status->getDetails());
    }

    public function testGetStatusException()
    {
        $mockClient = Mockery::mock(Client::CLASS);
        $mockClient->shouldReceive('info')->once()->withNoArgs()->andThrow(\Exception::CLASS, 'Error Message');

        $connector = Mockery::mock(
            ElasticsearchConnector::CLASS . '[getConnection]',
            ['connectorname', new ArrayConfig(['status_test' => 'info'])]
        );
        $connector->shouldReceive('getConnection')->once()->andReturn($mockClient);

        $status = $connector->getStatus();
        $this->assertFalse($status->isWorking());
        $this->assertEquals(['message' => 'Error on "info": Error Message'], $status->getDetails());
    }
}
