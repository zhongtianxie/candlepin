require 'spec_helper'
require 'candlepin_scenarios'
require 'thread'

describe 'Hypervisor Resource - Heartbeat Endpoint', :type => :virt do
  include CandlepinMethods
  include VirtHelper
  include AttributeHelper

  before(:each) do
    skip("candlepin running in standalone mode") if is_hosted?
    @expected_host_hyp_id = random_string("host")
    @expected_reporter_id = random_string("reporter")
    @expected_host_name = random_string("name")
    @expected_guest_ids = [@uuid1, @uuid2]

    # we must register the consumer to use it as a client
    # hypervisor check in creation does not result in a client cert
    @consumer = @user.register(@expected_host_name, :hypervisor, nil, {"test_fact" => "fact_value"},
                              nil, nil, [], [], nil, [], @expected_host_hyp_id, @expected_reporter_id, last_checkin_date = '1518341313')

    @client = consumer_client(@user, random_string("consumer"))
  end

  it 'should update last checkin date of consumers of given reporter' do
    @client.hypervisor_heartbeat_update(@owner['key'], @expected_reporter_id)

    # Wait for job to finish
    sleep 1

    consumer = @cp.get_consumer(@consumer.uuid)

    Date.parse(consumer.lastCheckin).should == Date.today
  end

end
