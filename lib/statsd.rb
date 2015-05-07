require 'socket'
require 'zlib'

# = Statsd: A Statsd client (https://github.com/etsy/statsd)
#
# @example Set up a global Statsd client for a server on localhost:8125
#   $statsd = Statsd.new 'localhost', 8125
# @example Send some stats
#   $statsd.increment 'garets'
#   $statsd.timing 'glork', 320
# @example Use {#time} to time the execution of a block
#   $statsd.time('account.activate') { @account.activate! }
# @example Create a namespaced statsd client and increment 'account.activate'
#   statsd = Statsd.new('localhost').tap{|sd| sd.namespace = 'account'}
#   statsd.increment 'activate'
class Statsd
  class RubyUdpClient
    attr_reader :key, :sock

    def initialize(address, port, key = nil)
      addrinfo = Addrinfo.ip(address)
      @sock = UDPSocket.new(addrinfo.pfamily)
      @sock.connect(addrinfo.ip_address, port)
      @key = key
    end

    def send(msg)
      sock.write(msg)
    rescue => boom
      nil
    end
  end

  # A namespace to prepend to all statsd calls.
  attr_accessor :namespace

  # All the endpoints where StatsD will report metrics
  attr_reader :shards

  #characters that will be replaced with _ in stat names
  RESERVED_CHARS_REGEX = /[\:\|\@]/

  def initialize(client_class = nil)
    @shards = []
    @client_class = client_class || RubyUdpClient
  end

  def self.simple(addr, port = nil)
    self.new.add_shard(addr, port)
  end

  def add_shard(addr, port = nil, key = nil)
    addr, port = addr.split(':') if addr.include?(':')
    @shards << @client_class.new(addr, port.to_i, key)
    self
  end

  # Sends an increment (count = 1) for the given stat to the statsd server.
  #
  # @param stat (see #count)
  # @param sample_rate (see #count)
  # @see #count
  def increment(stat, sample_rate=1); count stat, 1, sample_rate end

  # Sends a decrement (count = -1) for the given stat to the statsd server.
  #
  # @param stat (see #count)
  # @param sample_rate (see #count)
  # @see #count
  def decrement(stat, sample_rate=1); count stat, -1, sample_rate end

  # Sends an arbitrary count for the given stat to the statsd server.
  #
  # @param [String] stat stat name
  # @param [Integer] count count
  # @param [Integer] sample_rate sample rate, 1 for always
  def count(stat, count, sample_rate=1); send stat, count, 'c', sample_rate end

  # Sends an arbitary gauge value for the given stat to the statsd server.
  #
  # @param [String] stat stat name.
  # @param [Numeric] gauge value.
  # @example Report the current user count:
  #   $statsd.gauge('user.count', User.count)
  def gauge(stat, value)
    send stat, value, 'g'
  end

  # Sends a timing (in ms) for the given stat to the statsd server. The
  # sample_rate determines what percentage of the time this report is sent. The
  # statsd server then uses the sample_rate to correctly track the average
  # timing for the stat.
  #
  # @param stat stat name
  # @param [Integer] ms timing in milliseconds
  # @param [Integer] sample_rate sample rate, 1 for always
  def timing(stat, ms, sample_rate=1); send stat, ms, 'ms', sample_rate end

  # Reports execution time of the provided block using {#timing}.
  #
  # @param stat (see #timing)
  # @param sample_rate (see #timing)
  # @yield The operation to be timed
  # @see #timing
  # @example Report the time (in ms) taken to activate an account
  #   $statsd.time('account.activate') { @account.activate! }
  def time(stat, sample_rate=1)
    start = Time.now
    result = yield
    timing(stat, ((Time.now - start) * 1000).round(5), sample_rate)
    result
  end

  # Sends a histogram measurement for the given stat to the statsd server. The
  # sample_rate determines what percentage of the time this report is sent. The
  # statsd server then uses the sample_rate to correctly track the average
  # for the stat.
  def histogram(stat, value, sample_rate=1); send stat, value, 'h', sample_rate end

  private

  def sampled(sample_rate)
    yield unless sample_rate < 1 and rand > sample_rate
  end

  def send(stat, delta, type, sample_rate=1)
    sampled(sample_rate) do
      prefix = "#{@namespace}." unless @namespace.nil?
      stat = stat.to_s.gsub('::', '.').gsub(RESERVED_CHARS_REGEX, '_')
      msg = "#{prefix}#{stat}:#{delta}|#{type}#{'|@' << sample_rate.to_s if sample_rate < 1}"
      shard = select_shard(stat)
      shard.send(shard.key ? signed_payload(shard.key, msg) : msg)
    end
  end

  def select_shard(stat)
    if @shards.size == 1
      @shards.first
    else
      @shards[Zlib.crc32(stat) % @shards.size]
    end
  end

  def signed_payload(key, message)
    sha256 = Statsd.setup_openssl
    payload = timestamp + nonce + message
    signature = OpenSSL::HMAC.digest(sha256, key, payload)
    signature + payload
  end

  # defer loading openssl and securerandom unless needed. this shaves ~10ms off
  # of baseline require load time for environments that don't require message signing.
  def self.setup_openssl
    @sha256 ||= begin
      require 'securerandom'
      require 'openssl'
      OpenSSL::Digest::SHA256.new
    end
  end

  def timestamp
    [Time.now.to_i].pack("Q<")
  end

  def nonce
    SecureRandom.random_bytes(4)
  end
end
