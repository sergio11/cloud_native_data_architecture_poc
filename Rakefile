# frozen_string_literal: true

# ==============================================================================
# Modern Data Architecture POC - Master Rakefile
#
# THERE IS NO LOCAL EXECUTION PROFILE. The platform runs on Kubernetes only
# (kind + Podman, per srs.txt RNF-04): ingestion is a CronJob, the medallion
# layers are SparkApplications, the speed layer is a FlinkDeployment, and
# serving is Trino. There is deliberately no task that executes the pipeline
# locally, because a local run would be a different system pretending to be
# the real one.
#
# What remains local is code verification only (tests, lint, types, contract
# validation) plus cluster operations. Every cluster task is plain Ruby that
# drives podman/kind/kubectl/helm directly - there are no shell scripts, so
# the tasks behave the same on Windows (no bash/WSL dependency).
# ==============================================================================

require 'rake'
require 'open3'
require 'tempfile'
require 'yaml'

# Interleave our output with the child processes' (kubectl/helm write
# unbuffered; without this the sections appear out of order when piped).
$stdout.sync = true

def preflight_require!(cmd, name, remedy)
  sh cmd
rescue StandardError
  abort "\e[31mPre-flight failed: #{name} check did not run: #{cmd}\e[0m\n\e[33m#{remedy}\e[0m"
end

# ------------------------------------------------------------------------------
# Output helpers
# ------------------------------------------------------------------------------
def info_msg(msg)    = puts("\e[34m[INFO]\e[0m #{msg}")
def ok_msg(msg)      = puts("\e[32m[ OK ]\e[0m #{msg}")
def warn_msg(msg)    = puts("\e[33m[WARN]\e[0m #{msg}")
def err_msg(msg)     = puts("\e[31m[FAIL]\e[0m #{msg}")

def banner(title)
  puts "\e[36m== #{title} #{'=' * [70 - title.length, 0].max}\e[0m"
end

# ------------------------------------------------------------------------------
# Command helpers
# ------------------------------------------------------------------------------
# Runs a command capturing combined output. Never raises: returns [output, ok?].
# Argument arrays are passed straight to the process (no shell interpolation).
def capture(*cmd, stdin: nil)
  out, status = if stdin
                  Open3.capture2e(*cmd, stdin_data: stdin)
                else
                  Open3.capture2e(*cmd)
                end
  [out.to_s, status.success?]
rescue Errno::ENOENT
  ["#{cmd.first}: command not found", false]
end

# Runs a command streaming its output; aborts the task with a clean message.
def run!(*cmd)
  sh(*cmd)
rescue StandardError
  abort "\e[31mFAILED: #{cmd.join(' ')}\e[0m"
end

# ------------------------------------------------------------------------------
# Podman / kind discovery
# ------------------------------------------------------------------------------
# The kind cluster may live on the rootful connection (`podman-machine-default-root`)
# or the rootless one; rootless `podman ps` and `kind get clusters` are blind to
# rootful containers, so BOTH connections are always probed (rootful first).
KIND_LABEL = 'io.x-k8s.kind.cluster'

def podman_connections
  out, ok = capture('podman', 'system', 'connection', 'list', '--format', '{{.Name}}')
  return [] unless ok

  out.lines.map(&:strip).reject(&:empty?).sort_by { |n| n.end_with?('-root') ? 0 : 1 }
end

# Probes every podman connection once. Returns:
#   connections: all configured connection names
#   reachable:   connections whose daemon/machine answers
#   containers:  kind cluster containers as {connection:, name:, status:}
def podman_probe
  connections = podman_connections
  reachable = []
  containers = []
  connections.each do |conn|
    out, ok = capture('podman', '--connection', conn, 'ps', '-a',
                      '--filter', "label=#{KIND_LABEL}",
                      '--format', '{{.Names}}|{{.Status}}')
    next unless ok

    reachable << conn
    out.lines.each do |line|
      name, status = line.strip.split('|', 2)
      next if name.nil? || name.empty?

      containers << { connection: conn, name: name, status: status.to_s }
    end
  end
  { connections: connections, reachable: reachable, containers: containers }
end

# `podman ps --format {{.Status}}` reports "Up 5 minutes" / "Exited (0) ..."
# (unlike Docker, it never says "Running").
def container_running?(container) = container[:status].start_with?('Up')

# How many kind nodes belong to <name>. Used only to report what is about to be
# deleted; returns 0 rather than raising when the cluster is already gone.
def probe_count(cluster_name)
  probe = podman_probe
  probe[:containers].count { |c| c[:name].start_with?("#{cluster_name}-") }
end

# Applies the storage/compute domain roles to the kind workers, by name.
#
# The roles cannot live in cluster-config.yaml: kind turns them into the
# kubelet's --node-labels, and a kubelet that gets a kubernetes.io/ label it
# does not recognise exits with "failed to validate kubelet flags" instead of
# booting. See infra/kind/cluster-config.yaml for the full kubelet message.
#
# kind names workers <cluster>-worker, <cluster>-worker2, ... in order, so the
# index decides the role. Missing nodes are reported, never fatal: a
# single-node or two-node cluster is a legitimate shape.
def label_worker_roles(cluster_name)
  info_msg('labelling kind workers with their domain roles...')
  roles = { 'worker' => 'storage', 'worker2' => 'compute' }
  labelled = 0

  roles.each do |suffix, role|
    node = "#{cluster_name}-#{suffix}"
    exists, = capture('kubectl', 'get', 'node', node)
    unless exists
      warn_msg("node #{node} is not in the cluster - skipping its '#{role}' role")
      next
    end

    out, ok = capture('kubectl', 'label', 'node', node,
                      "node-role.kubernetes.io/#{role}=true",
                      'node-role.kubernetes.io/worker=true',
                      '--overwrite')
    if ok
      ok_msg("#{node} -> #{role}")
      labelled += 1
    else
      warn_msg("could not label #{node}: #{out.to_s.lines.first}")
    end
  end

  if labelled.zero?
    warn_msg('no worker was labelled')
  else
    sh 'kubectl', 'get', 'nodes', '--show-labels' rescue nil
  end
end

# Imports one image into one kind node, from the machine's own store.
# Returns true when the node ends up holding it under the requested name.
def load_image_into(node, image)
  listing, ok = capture('podman', '--connection', node[:connection], 'images',
                        '--format', '{{.Repository}}:{{.Tag}} {{.Id}}')
  abort "\e[31mpodman is not reachable on connection #{node[:connection]}\e[0m" unless ok

  # Podman does NOT store an unqualified registry the way Docker does: a
  # `podman build -t poc/flink-stream:0.1.0` lands as
  # localhost/poc/flink-stream:0.1.0, so a lookup for the docker.io/ form finds
  # nothing and the task reports "not built yet" for an image that was built
  # seconds earlier. Compare on repository:tag with the registry prefix removed
  # from BOTH sides, so docker.io/x, localhost/x and x all match.
  wanted = short_image_ref(image)
  entry = listing.lines.find { |line| short_image_ref(line.split.first.to_s) == wanted }
  if entry.nil?
    warn_msg("#{image} is not built yet - build it with podman build first")
    warn_msg("  store has: #{listing.lines.map { |l| l.split.first }.reject(&:empty?).join(', ')}")
    return false
  end

  # podman reports the image config digest, which is the same digest containerd
  # stores as a blob, so this is a precise identity check. `ctr images list`
  # cannot be used for it: that column is the MANIFEST digest, which changes on
  # every re-save and is not what the pod pins.
  store_ref = entry.split.first
  short_id = entry.split[1][0, 12]

  #  Already there, with THIS digest, under the right name? Nothing to do.
  #
  #  Both conditions are required. The name alone is not enough: a rebuild
  #  reuses poc/flink-stream:0.1.0, so the node still holds that tag pointing at
  #  the PREVIOUS image id and the task would skip the copy and leave the
  #  cluster running yesterday's code.
  node_images, listed = capture('podman', '--connection', node[:connection], 'exec',
                                node[:name], 'ctr', '-n', 'k8s.io', 'images', 'ls', '-q')
  node_blobs, bl = capture('podman', '--connection', node[:connection], 'exec',
                           node[:name], 'ctr', '-n', 'k8s.io', 'content', 'ls', '-q')
  has_name = listed && node_images.lines.map(&:strip).include?(image)
  has_id = bl && node_blobs.include?("sha256:#{short_id}")
  if has_name && has_id
    ok_msg("#{image} already in #{node[:name]} (#{short_id}) - skipped")
    return true
  end
  if has_name && !has_id
    info_msg("#{image} is in #{node[:name]} but at a different image id - reloading")
  end

  info_msg("#{image} -> #{node[:name]} (image #{short_id}, copying inside the machine)")
  # Two phases so progress is measurable: the save grows the tar, then the
  # import grows the node blob store. One piped command hides both and looks
  # exactly like a hang.
  #
  # The tar is named after the image id and KEPT in the machine. podman save of
  # this image costs ~5 minutes and most of it is layers the node already has,
  # so re-saving on every retry threw that away over and over. Keyed by id, a
  # retry after a failed import pays only the import.
  tar = "/tmp/kind-load-#{short_id}.tar"

  info_msg('  phase 1/2: podman save inside the machine')
  # test -s first: `podman save -o` refuses to touch an existing docker-archive
  # ("docker-archive doesn't support modifying existing images"), so a tar left
  # behind by an interrupted run would otherwise break every subsequent run.
  save_out, saved = machine_script("test -s #{tar} && echo CACHED || " \
                                   "(rm -f #{tar}; podman save -o #{tar} #{store_ref})") do |_tick|
    size = machine_probe("stat -c %s #{tar} 2>/dev/null || echo 0")
    warn_msg("  save: #{(size.to_i / 1_048_576).to_s.rjust(5)} MB written")
  end
  unless saved
    err_msg("#{image}: podman save failed")
    puts save_out
    return false
  end
  final_size = machine_probe("stat -c %s #{tar} 2>/dev/null || echo 0").to_i
  if final_size.zero?
    err_msg("#{image}: no tar produced by the save")
    puts save_out
    return false
  end
  ok_msg("  tar ready: #{final_size / 1_048_576} MB in the machine")

  info_msg("  phase 2/2: ctr images import into #{node[:name]}")
  # The tar is deliberately NOT removed: it is the cache that makes the next
  # attempt cost only the import. `cluster:image:clean` removes it.
  out, success = machine_script("cat #{tar} | podman exec -i #{node[:name]} " \
                                'ctr -n k8s.io images import -') do |_tick|
    # Bytes, not blob count: `ctr images import` stages the whole tar before it
    # commits anything, and almost every layer is already in the node
    # (content addressed), so the new-blob delta stays at 0 through a transfer
    # that is actually running at full speed.
    used, = capture('podman', '--connection', node[:connection], 'exec', node[:name],
                    'df', '--output=used', '-B1', '/var/lib/containerd')
    warn_msg("  import: #{used.to_s.strip.split("\n").last.to_s.strip.to_i / 1_048_576} MB used by the node store")
  end

  # Phase 3: alias the image under the docker.io/ name the charts use.
  #
  # podman stores an unqualified build as localhost/poc/flink:0.1.0, and
  # `ctr images import` preserves that spelling. Kubernetes resolves a bare
  # reference to docker.io/ FIRST: the FlinkDeployment says
  # `image: poc/flink-stream:0.1.0`, the kubelet turns it into
  # docker.io/poc/flink-stream:0.1.0, finds no such tag and fails with
  #   failed to resolve reference: pull access denied ... insufficient_scope
  # even though the image is sitting right there in the node.
  #
  # The tag is content-addressed, so the alias costs no bytes. Doing it here
  # keeps the charts portable (they keep saying docker.io/poc/...) instead of
  # hardcoding localhost/ into every image reference.
  if store_ref != image
    tag_out, tagged = capture('podman', '--connection', node[:connection], 'exec',
                              node[:name], 'ctr', '-n', 'k8s.io', 'images',
                              'tag', store_ref, image)
    if tagged
      ok_msg("  aliased in the node: #{store_ref} -> #{image}")
    else
      warn_msg("could not tag #{image} in the node: #{tag_out.to_s.lines.first}")
    end
  end

  verify, checked = capture('podman', '--connection', node[:connection], 'exec',
                            node[:name], 'ctr', '-n', 'k8s.io', 'content', 'ls', '-q')
  present = checked && verify.include?("sha256:#{short_id}")
  unless success && present
    err_msg("#{image} NOT loaded into #{node[:name]}#{success ? '' : ' (import command failed)'} - #{short_id} absent")
    puts out unless out.to_s.strip.empty?
    return false
  end
  ok_msg("#{image} loaded into #{node[:name]} (#{short_id})")
  true
end

# Reduces an image reference to repository:tag with any registry prefix removed.
#
# Podman resolves an unqualified registry (podman build -t poc/flink:0.1.0) to
# localhost/poc/flink:0.1.0 and keeps the fully-qualified form only if you
# write it yourself, so the same image can be listed under either name. The
# default port is dropped too, since :5000/... is the other spelling.
#
#   docker.io/poc/flink:0.1.0  -> poc/flink:0.1.0
#   localhost/poc/flink:0.1.0   -> poc/flink:0.1.0
#   poc/flink:0.1.0            -> poc/flink:0.1.0
#   docker.io/library/busybox:1.36 -> library/busybox:1.36
def short_image_ref(ref)
  repo, _, tag = ref.to_s.partition(':')
  repo = repo.sub(%r{\A(localhost|docker\.io|index\.docker\.io|registry-1\.docker\.io)/}, '')
  repo = repo.sub(%r{:\d+/}, '/')
  tag.empty? ? repo : "#{repo}:#{tag}"
end

# Names of the kind clusters that exist right now.
# `kind` prints "enabling experimental podman provider" (once, when it activates
# the podman provider) and "No kind clusters found." on the same stream as the
# list, so the raw output has to be filtered or both of those lines are read as
# cluster names. Returns [] when kind is missing or there is nothing to report.
def kind_clusters
  out, ok = capture('kind', 'get', 'clusters')
  return [] unless ok

  out.lines.map(&:strip).reject(&:empty?).reject do |line|
    line.start_with?('enabling experimental podman provider') ||
      line.casecmp?('No kind clusters found.')
  end
end

# Runs a shell script INSIDE the podman machine, printing elapsed time every
# 15 seconds. Three reasons this exists instead of a bare `sh`:
#
#   * Transferring a 1.3 GB image is silent until it finishes, and a silent
#     copy is indistinguishable from a hang - so the task must show a heartbeat.
#   * Doing the copy from the Windows shell sends every byte across the ssh
#     tunnel twice: VM -> ssh -> host -> ssh -> VM. Running the pipeline inside
#     the machine keeps `podman save` reading the machine's own store and never
#     touches the tunnel at all.
#   * `podman machine ssh` JOINS its arguments with spaces and hands the result
#     to a remote shell. A multi-line script is therefore split at every `;` and
#     newline: `sudo -n sh -c 'set -e; podman save ...'` degenerates into
#     `sudo -n sh -c set -e; podman save ...`, where only the first command runs
#     as root and the rest silently run as the ssh user against the wrong store
#     ("image not known"). The script is therefore collapsed to ONE line and
#     wrapped in single quotes so it survives as a single remote argument.
#
# `podman machine ssh` reaches the machine behind the DEFAULT connection; both
# configured connections point at the same machine, so which one is default does
# not change what is read here. Inside the VM the images and the kind container
# live in root's store, hence sudo.
def machine_script(script)
  one_line = script.gsub(/\s+/, ' ').strip
  if one_line.include?("'")
    raise ArgumentError, 'machine_script: the script must not contain single quotes (they would end the remote quoting)'
  end

  out = +''
  status = nil
  reader = Thread.new do
    Open3.popen2e('podman', 'machine', 'ssh', 'sudo', '-n', 'sh', '-c', "'#{one_line}'") do |stdin, pipe, thread|
      stdin.close
      pipe.each_line { |line| out << line }
      status = thread.value
    end
  end
  started = Time.now
  until reader.join(15)
    yield "transferring... #{Time.now - started} s elapsed"
  end
  reader.value
  [out, status&.success?]
end

# Byte-exact progress for a machine script that streams a file, so a slow copy
# is visibly progressing instead of looking like a hang. `probe` is a script
# run on every heartbeat and must print a single short line.
def machine_probe(probe)
  out, = capture('podman', 'machine', 'ssh', 'sudo', '-n', 'sh', '-c', "'#{probe.gsub(/\s+/, ' ').strip}'")
  out.lines.last.to_s.strip
end

# ------------------------------------------------------------------------------
# Kubernetes / Helm helpers
# ------------------------------------------------------------------------------
def api_server
  out, ok = capture('kubectl', 'config', 'view', '--minify',
                     '-o', 'jsonpath={.clusters[0].cluster.server}')
  ok && !out.strip.empty? ? out.strip : nil
end

def api_up?
  _, ok = capture('kubectl', 'get', 'nodes', '--request-timeout=5s', '-o', 'name')
  ok
end

# kind serves a self-signed certificate on 127.0.0.1; every manual command in
# this repo uses this flag, so all Helm invocations go through the same constant.
HELM_KUBE_ARGS = ['--kube-insecure-skip-tls-verify'].freeze

# The Helm releases each startup profile installs (release name == chart dir,
# namespace platform). Mirrors the contract documented in infra/profiles/*.yaml.
PROFILE_CHARTS = {
  'platform' => %w[s1-ingestion s2-lakehouse s7-fabric],
  'stream'   => %w[s1-ingestion s2-lakehouse s4-stream s5-serving],
  'batch'    => %w[s1-ingestion s2-lakehouse s3-batch s5-serving],
  'demo'     => %w[s1-ingestion s2-lakehouse s3-batch s4-stream s5-serving
                   s6-mesh s7-fabric s8-ops]
}.freeze

# Extracts `subsystems.<chart>` from infra/profiles/<profile>.yaml into a
# Tempfile for `helm upgrade -f`. Returns the Tempfile (caller unlinks), nil
# when the profile has no section for the chart (install with chart defaults),
# or :error when the profile cannot be read/parsed.
def extract_profile_values(profile, chart)
  doc = YAML.load_file(File.join('infra', 'profiles', "#{profile}.yaml"))
  subsystems = doc.is_a?(Hash) ? doc['subsystems'] : nil
  section = subsystems.is_a?(Hash) ? subsystems[chart] : nil
  return nil if section.nil?

  tmp = Tempfile.new(["#{chart}-", '.yaml'])
  tmp.write(YAML.dump(section))
  tmp.close
  tmp
rescue Psych::Exception, Errno::ENOENT
  :error
end

# Streams `helm upgrade --install` for one chart. Returns true/false.
def helm_upgrade(chart, values_path)
  cmd = ['helm', 'upgrade', '--install', chart, "infra/helm/#{chart}",
         '-n', 'platform', *HELM_KUBE_ARGS]
  cmd += ['-f', values_path] if values_path
  sh(*cmd)
  true
rescue StandardError
  false
end

desc 'Default task: run the code-verification suite (the platform itself runs on Kubernetes)'
task default: %i[governance:validate lint typecheck test]

desc 'Verify system pre-flight dependencies and Python environment'
task :preflight do
  puts "\e[33m[Pre-flight] Environment checks...\e[0m"
  preflight_require!('python --version', 'Python',
                     'Install Python 3.11+ and make sure `python` is on PATH.')
  preflight_require!('rake --version', 'Rake',
                     'Install Rake (gem install rake) and make sure `rake` is on PATH.')
  preflight_require!('python -m pytest --version', 'pytest',
                     'Install the dev toolchain: pip install pytest pytest-cov (pyproject [project.optional-dependencies].dev).')
  puts "\e[32mPython runtime, Rake and pytest verified.\e[0m"
end

desc 'Run all automated tests (unit + integration) with the coverage gate'
task :test do
  puts "\e[33m[Tests] Executing automated suite...\e[0m"
  sh 'python -m pytest tests --cov=src --cov=lineage --cov-branch --cov-fail-under=85 -q'
  puts "\e[32mAll test suites passed.\e[0m"
end

namespace :test do
  desc 'Run unit tests'
  task :unit do
    sh 'python -m pytest tests/unit -v'
  end

  desc 'Run integration tests (RF acceptance scenarios)'
  task :integration do
    sh 'python -m pytest tests/integration -v'
  end

  desc 'Run tests with a coverage report'
  task :coverage do
    sh 'python -m pytest tests --cov=src --cov=lineage --cov-config=pyproject.toml --cov-report=term-missing'
  end
end

desc 'Lint the codebase with ruff'
task :lint do
  puts "\e[33m[Quality] Running ruff...\e[0m"
  sh 'ruff check src tests pipelines governance quality data-generator lineage'
  puts "\e[32mRuff: all checks passed.\e[0m"
end

desc 'Format the codebase with ruff (same directories as lint, writes in place)'
task :format do
  puts "\e[33m[Quality] Running ruff format...\e[0m"
  sh 'ruff format src tests pipelines governance quality data-generator lineage'
  puts "\e[32mRuff format: done.\e[0m"
end

namespace :format do
  desc 'Verify formatting without writing (CI gate: rake format:check)'
  task :check do
    sh 'ruff format --check src tests pipelines governance quality data-generator lineage'
    puts "\e[32mRuff format: all files formatted.\e[0m"
  end
end

desc 'Type-check the codebase with mypy'
task :typecheck do
  puts "\e[33m[Quality] Running mypy...\e[0m"
  sh 'python -m mypy'
  puts "\e[32mMypy: no errors.\e[0m"
end

namespace :governance do
  desc 'Validate Data Products and Contracts across domains (Data Mesh)'
  task :validate do
    puts "\e[33m[Governance] Validating Data Products & Contracts (Data Mesh / ODCS)...\e[0m"
    sh 'python governance/contract_validator.py --repo-root .'
    puts "\e[32mData contracts verified.\e[0m"
  end
end

# ---------------------------------------------------------------------------
# Cluster operations (Kubernetes). These do not run the pipelines; they
# deploy and observe them. kind runs on Podman - possibly the rootful
# connection - so discovery always probes every connection.
# ---------------------------------------------------------------------------
namespace :cluster do
  desc 'Guided status: kind container, API, nodes, pods, Helm releases + next step'
  task :status do
    banner 'CLUSTER STATUS'
    next_step = nil
    probe = podman_probe

    # 1. kind container (any podman connection)
    if probe[:connections].empty?
      err_msg('podman is not available or has no connections configured')
      next_step = 'Install podman (podman machine init && podman machine start)'
    elsif probe[:reachable].empty?
      err_msg("podman machine is not running (checked: #{probe[:connections].join(', ')})")
      next_step = 'podman machine start'
    elsif probe[:containers].empty?
      warn_msg("no kind cluster container on: #{probe[:reachable].join(', ')}")
      next_step = 'rake cluster:setup   # detect or create the cluster'
    else
      probe[:containers].each do |c|
        line = "container #{c[:name]} (#{c[:connection]}): #{c[:status]}"
        container_running?(c) ? ok_msg(line) : err_msg(line)
      end
      next_step = 'rake cluster:start' if probe[:containers].none? { |c| container_running?(c) }
    end

    # 2. API server
    server = api_server
    server ? info_msg("API server: #{server}") : warn_msg('no API server in the current kubeconfig context')
    api = api_up?
    if api
      ok_msg('API reachable')
    else
      err_msg('API unreachable (kubectl cannot reach the server)')
      next_step ||= 'rake cluster:start'
    end

    if api
      # 3. Nodes
      puts "\n\e[33mNodes:\e[0m"
      sh 'kubectl', 'get', 'nodes' rescue warn_msg('kubectl get nodes failed')

      # 4. Pods: list everything, then flag the ones that are not healthy
      puts "\n\e[33mPods:\e[0m"
      pods_out, pods_ok = capture('kubectl', 'get', 'pods', '-A', '--no-headers')
      if pods_ok
        sh 'kubectl', 'get', 'pods', '-A' rescue nil
        unhealthy = pods_out.lines.filter_map do |line|
          cols = line.split(/\s+/)
          next nil if cols.size < 6

          name, ns, ready, status = cols[0], cols[1], cols[2], cols[3]
          if %w[Running Succeeded Completed].include?(status)
            next nil if status != 'Running'

            want, have = ready.split('/').map(&:to_i)
            next nil if want == have

            { ns: ns, name: name, why: "Running but not Ready (#{ready})" }
          else
            { ns: ns, name: name, why: status }
          end
        end
        if unhealthy.empty?
          ok_msg('all pods healthy')
        else
          warn_msg("#{unhealthy.size} pod(s) not healthy:")
          unhealthy.each { |p| puts "      - #{p[:ns]}/#{p[:name]}: #{p[:why]}" }
          first = unhealthy.first
          next_step ||= "kubectl describe pod -n #{first[:ns]} #{first[:name]}  |  rake cluster:logs[#{first[:name]}]"
        end
      else
        warn_msg("could not list pods: #{pods_out.lines.first}")
      end

      # 5. Helm releases
      puts "\n\e[33mHelm releases (namespace platform):\e[0m"
      helm_out, helm_ok = capture('helm', 'list', '-n', 'platform', *HELM_KUBE_ARGS)
      puts helm_out
      warn_msg('helm list failed') unless helm_ok
    end

    puts
    if next_step
      warn_msg("Next step: #{next_step}")
    else
      ok_msg('Cluster is healthy. Detail: kubectl get pods -A Â· helm history <release> -n platform Â· rake cluster:logs[<name>]')
    end
  end

  desc 'Start the kind cluster container and wait for the API and nodes'
  task :start do
    banner 'CLUSTER START'
    probe = podman_probe
    if probe[:connections].empty?
      abort "\e[31mpodman is not available. Install it and run: podman machine init && podman machine start\e[0m"
    end
    if probe[:reachable].empty?
      abort "\e[31mpodman machine is not running. Run: podman machine start\e[0m"
    end
    if probe[:containers].empty?
      abort "\e[31mNo kind cluster container found. Run: rake cluster:setup\e[0m"
    end

    probe[:containers].each do |c|
      if container_running?(c)
        ok_msg("container #{c[:name]} already running (#{c[:connection]})")
      else
        info_msg("starting #{c[:name]} (currently: #{c[:status]}) via #{c[:connection]}...")
        run! 'podman', '--connection', c[:connection], 'start', c[:name]
        ok_msg("container #{c[:name]} started")
      end
    end

    info_msg("waiting for the API (#{api_server || 'endpoint unknown'})...")
    deadline = Time.now + 120
    last_error = nil
    until api_up?
      if Time.now > deadline
        err_msg('API did not become reachable within 120 s')
        warn_msg("last kubectl error: #{last_error&.lines&.last&.strip}")
        abort "\e[33mInspect the control plane: podman logs #{probe[:containers].first[:name]} --tail 50\e[0m"
      end
      last_error, = capture('kubectl', 'get', 'nodes', '--request-timeout=5s')
      sleep 3
    end
    ok_msg('API reachable')

    run! 'kubectl', 'wait', '--for=condition=Ready', 'nodes', '--all', '--timeout=180s'
    ok_msg('all nodes Ready')
    puts "\n\e[33mNext step: rake cluster:status\e[0m"
  end

  desc 'Stop the kind cluster container (RestartPolicy=no: it does not auto-restart)'
  task :stop do
    banner 'CLUSTER STOP'
    probe = podman_probe
    running = probe[:containers].select { |c| container_running?(c) }
    if running.empty?
      ok_msg('no kind cluster container is running')
      next
    end
    running.each do |c|
      info_msg("stopping #{c[:name]} via #{c[:connection]}...")
      run! 'podman', '--connection', c[:connection], 'stop', c[:name]
      ok_msg("#{c[:name]} stopped")
    end
    warn_msg('RestartPolicy=no: the cluster will NOT come back on host/WSL reboot - use rake cluster:start')
  end

  desc 'Tail the logs of the pod whose name contains <name> (args: cluster:logs[<substring>])'
  task :logs, [:name] do |_t, args|
    target = args[:name].to_s
    abort "\e[31mUsage: rake cluster:logs[<pod-name-substring>]\e[0m" if target.empty?

    pods_out, ok = capture('kubectl', 'get', 'pods', '-A', '--no-headers')
    abort "\e[31mCannot list pods: #{pods_out.lines.first}\e[0m" unless ok

    matches = pods_out.lines.filter_map do |line|
      cols = line.split(/\s+/)
      next nil if cols.size < 6

      { name: cols[0], ns: cols[1] } if cols[0].include?(target)
    end
    case matches.size
    when 0
      abort "\e[31mNo pod matches '#{target}'. List them with: kubectl get pods -A\e[0m"
    when 1
      pod = matches.first
      containers_out, c_ok = capture('kubectl', 'get', 'pod', '-n', pod[:ns], pod[:name],
                                     '-o', 'jsonpath={.spec.containers[*].name}')
      containers = c_ok ? containers_out.split : []
      if containers.size > 1
        abort "\e[31mPod #{pod[:ns]}/#{pod[:name]} has several containers (#{containers.join(', ')}).\n" \
              "Run: kubectl logs -n #{pod[:ns]} #{pod[:name]} -c <container>\e[0m"
      end
      info_msg("logs: #{pod[:ns]}/#{pod[:name]} (Ctrl+C to stop)")
      run! 'kubectl', 'logs', '-n', pod[:ns], pod[:name], '--tail=200', '-f'
    else
      err_msg("'#{target}' matches several pods:")
      matches.each { |m| puts "      - #{m[:ns]}/#{m[:name]}" }
      abort "\e[33mBe more specific: rake cluster:logs[<longer substring>]\e[0m"
    end
  end

  desc 'Verify prerequisites, detect-or-create the kind cluster, namespaces, policies, platform profile'
  task :setup do
    banner 'CLUSTER SETUP'
    Rake::Task['cluster:create'].invoke
    Rake::Task['cluster:up'].invoke('platform')
    puts
    sh 'kubectl', 'get', 'pods', '-A' rescue nil
    ok_msg('Cluster setup finished. Follow with: rake cluster:status')
  end

  desc 'Create (or reuse) the kind cluster: prereqs, nodes, namespaces, storage, Helm repos. No profile.'
  task :create do
    banner 'CLUSTER CREATE'

    # 1. Prerequisites
    info_msg('checking prerequisites (kubectl, helm, podman, kind)...')
    remedies = {
      'kubectl' => 'Install kubectl and make sure it is on PATH.',
      'helm'    => 'Install Helm and make sure it is on PATH.',
      'podman'  => 'Install Podman (podman machine init && podman machine start on Windows/macOS).',
      'kind'    => 'Install kind (https://kind.sigs.k8s.io/).'
    }
    { 'kubectl' => %w[version --client], 'helm' => %w[version --short],
      'podman' => %w[version], 'kind' => %w[version] }.each do |tool, args|
      _, ok = capture(tool, *args)
      abort "\e[31mPre-flight failed: #{tool} is not available.\e[0m\n\e[33m#{remedies[tool]}\e[0m" unless ok
    end
    ok_msg('prerequisites verified')

    # 2. Host persistence directories (kind extraMounts read them)
    %w[data/control-plane data/worker-storage data/worker-compute].each do |dir|
      mkdir_p dir
      info_msg("directory #{dir}")
    end

    # 3. Detect or create the cluster
    probe = podman_probe
    if probe[:reachable].empty?
      abort "\e[31mpodman machine is not running. Run: podman machine start, then rake cluster:setup\e[0m"
    end
    if probe[:containers].empty?
      info_msg('no kind cluster found - creating it from infra/kind/cluster-config.yaml (podman provider)...')
      ENV['KIND_EXPERIMENTAL_PROVIDER'] = 'podman'
      begin
        run! 'kind', 'create', 'cluster', '--config', 'infra/kind/cluster-config.yaml'
      ensure
        ENV.delete('KIND_EXPERIMENTAL_PROVIDER')
      end
      ok_msg('kind cluster created')
    else
      probe[:containers].each { |c| info_msg("reusing existing cluster container #{c[:name]} (#{c[:connection]})") }
    end

    # 4. Start (no-op when already running) and wait for API + nodes
    Rake::Task['cluster:start'].invoke

    # 4b. Domain roles on the kind workers.
    #
    # Applied here, by NODE NAME, and not in infra/kind/cluster-config.yaml:
    # kind turns a node's `labels:` into the kubelet's --node-labels flag, and
    # the kubelet refuses to start when that carries a kubernetes.io/ label
    # outside its allow-list ("failed to validate kubelet flags: unknown
    # 'kubernetes.io' or 'k8s.io' labels specified with --node-labels:
    # [node-role.kubernetes.io/storage node-role.kubernetes.io/worker]"). The
    # worker kubelet then crash-loops and never joins.
    #
    # By name, not by selector: a selector on node-role.kubernetes.io/storage
    # would match nothing, because that is the label being set in the first
    # place.
    label_worker_roles(ENV.fetch('CLUSTER_NAME', 'poc-data-platform'))

    # 5. Namespaces with domain/platform labels
    info_msg('configuring namespaces...')
    namespaces = {
      'platform' => 'Infrastructure, catalogs, message broker, object storage',
      'sales'    => 'Sales domain data products and pipelines',
      'marketing'=> 'Marketing domain data products and funnels',
      'finance'  => 'Finance domain data products and ledger',
      'ops'      => 'Observability, orchestration, and business intelligence'
    }
    namespaces.each do |ns, ns_desc|
      _, exists = capture('kubectl', 'get', 'namespace', ns)
      if exists
        info_msg("namespace #{ns} exists")
      else
        run! 'kubectl', 'create', 'namespace', ns
        ok_msg("namespace #{ns} created")
      end
      # Labels only, and only with values a label can hold: a label value must
      # match ([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9] - no spaces, no commas.
      # The description is prose ("Infrastructure, catalogs, message broker,
      # object storage"), so putting it in data-platform.io/description made
      # kubectl reject the whole command:
      #   invalid label value: ... a valid label must consist of alphanumeric
      #   characters, '-', '_' or '.'
      # which aborted cluster:create before any profile was deployed. Prose
      # belongs in an annotation.
      run! 'kubectl', 'label', 'namespace', ns,
           'data-mesh.io/domain=' + ns, 'data-platform.io/managed=true', '--overwrite'
      run! 'kubectl', 'annotate', 'namespace', ns,
           'data-platform.io/description=' + ns_desc, '--overwrite'
    end

    # 6. Kyverno governance policies (optional component: WARN, never abort)
    if Dir.exist?('governance/policies')
      _, applied = capture('kubectl', 'apply', '-f', 'governance/policies/')
      if applied
        ok_msg('governance admission policies applied')
      else
        warn_msg('could not apply governance/policies (Kyverno not installed?) - admission policies skipped')
      end
    end

    # 7. Storage: default StorageClass + local-path-fast (WaitForFirstConsumer)
    _, annotated = capture('kubectl', 'annotate', 'storageclass', 'standard',
                           'storageclass.kubernetes.io/is-default-class=true', '--overwrite')
    annotated ? ok_msg("storageclass 'standard' is the default") : warn_msg("could not annotate storageclass 'standard'")
    _, sc_applied = capture('kubectl', 'apply', '-f', '-', stdin: <<~YAML)
      apiVersion: storage.k8s.io/v1
      kind: StorageClass
      metadata:
        name: local-path-fast
      provisioner: rancher.io/local-path
      volumeBindingMode: WaitForFirstConsumer
      reclaimPolicy: Delete
    YAML
    sc_applied ? ok_msg("storageclass 'local-path-fast' present") : warn_msg("could not apply storageclass 'local-path-fast'")

    # 8. Helm repositories (network-dependent: WARN, never abort)
    {
      'strimzi' => 'https://strimzi.io/charts/',
      'minio' => 'https://charts.min.io/',
      'trino' => 'https://trinodb.github.io/charts',
      'dagster' => 'https://dagster-io.github.io/helm',
      'prometheus-community' => 'https://prometheus-community.github.io/helm-charts',
      'kyverno' => 'https://kyverno.github.io/kyverno/',
      'spark-operator' => 'https://kubeflow.github.io/spark-operator'
    }.each do |name, url|
      _, ok = capture('helm', 'repo', 'add', name, url, '--force-update')
      ok ? info_msg("helm repo #{name}") : warn_msg("could not reach helm repo '#{name}' (offline? local cache will be used)")
    end
    _, = capture('helm', 'repo', 'update')

    ok_msg('Cluster created. Deploy a profile with: rake cluster:up[stream]')
  end

  desc 'Delete the kind cluster and all its nodes. Irreversible: every pod, PVC and Helm release goes with it.'
  task :destroy do
    banner 'CLUSTER DESTROY'

    cluster_name = ENV.fetch('CLUSTER_NAME', 'poc-data-platform')
    unless ENV['YES'].to_s.strip.downcase == 'true'
      abort "\e[31mRefusing to delete the cluster '#{cluster_name}': every pod, PVC, " \
            "Helm release and the whole kind node will be removed.\e[0m\n" \
            "\e[33mRe-run with YES=true if that is what you want: rake cluster:destroy\e[0m"
    end

    _, kind_ok = capture('kind', 'version')
    unless kind_ok
      abort "\e[31mCould not run 'kind'. Is it on PATH?\e[0m"
    end

    existing = kind_clusters
    if existing.empty?
      ok_msg('no kind cluster exists - nothing to delete')
      next
    end
    unless existing.include?(cluster_name)
      abort "\e[31mCluster '#{cluster_name}' not found. Existing: #{existing.join(', ')}\e[0m"
    end

    info_msg("deleting cluster '#{cluster_name}' (nodes: #{probe_count(cluster_name)})...")
    # --kubeconfig is not passed on purpose: `kind delete` only needs the node
    # containers, and passing the kubeconfig makes it also rewrite the current
    # context, which loses the poc-qa context the operator may still be using.
    deleted, deleted_ok = capture('kind', 'delete', 'cluster', '--name', cluster_name)
    unless deleted_ok
      err_msg('kind delete failed:')
      puts deleted
      abort "\e[31mThe cluster was not deleted.\e[0m"
    end

    ok_msg("cluster '#{cluster_name}' deleted")
    left = kind_clusters
    left.empty? ? ok_msg('no kind clusters remain') : warn_msg("still present: #{left.join(', ')}")
    puts
    ok_msg('Recreate with: rake cluster:create')
  end

  desc "Deploy a startup profile with Helm (args: profile=platform|stream|batch|demo, default: stream)"
  task :up, [:profile] do |_t, args|
    profile = args[:profile] || 'stream'
    profile_file = File.join('infra', 'profiles', "#{profile}.yaml")
    unless PROFILE_CHARTS.key?(profile) && File.exist?(profile_file)
      abort "\e[31mUnknown profile: #{profile}. Choose from: #{PROFILE_CHARTS.keys.join(', ')}\e[0m"
    end
    _, helm_ok = capture('helm', 'version', '--short')
    abort "\e[31mhelm is not installed or not on PATH.\e[0m" unless helm_ok

    banner "ACTIVATING OPERATIONAL PROFILE: #{profile.upcase}"
    charts = PROFILE_CHARTS.fetch(profile)
    failures = 0
    deployed = 0
    tempfiles = []
    begin
      charts.each do |chart|
        unless File.directory?(File.join('infra', 'helm', chart))
          err_msg("MISSING: infra/helm/#{chart} not found")
          failures += 1
          next
        end
        info_msg("deploying Helm chart: #{chart}")
        values = extract_profile_values(profile, chart)
        if values == :error
          err_msg("could not extract 'subsystems.#{chart}' from #{profile_file}")
          failures += 1
          next
        end
        if values.nil?
          warn_msg("no '#{chart}' section in profile; installing with chart defaults")
        else
          tempfiles << values
          info_msg("profile section -> -f #{values.path}")
        end
        if helm_upgrade(chart, values&.path)
          ok_msg("#{chart}")
          deployed += 1
        else
          err_msg("#{chart}")
          failures += 1
        end
      end
    ensure
      tempfiles.each do |t|
        t.unlink
      rescue StandardError
        nil
      end
    end
    puts
    if failures.positive?
      abort "\e[31mProfile '#{profile}' FAILED: #{deployed} chart(s) deployed, #{failures} step(s) failed.\e[0m"
    end
    ok_msg("Profile '#{profile}' applied successfully! (#{deployed} chart(s) deployed)")
  end

  desc "Tear a startup profile down (args: profile=platform|stream|batch|demo, default: stream)"
  task :down, [:profile] do |_t, args|
    profile = args[:profile] || 'stream'
    unless PROFILE_CHARTS.key?(profile)
      abort "\e[31mUnknown profile: #{profile}. Choose from: #{PROFILE_CHARTS.keys.join(', ')}\e[0m"
    end
    banner "DEACTIVATING OPERATIONAL PROFILE: #{profile.upcase}"
    removed = 0
    failures = 0
    PROFILE_CHARTS.fetch(profile).each do |chart|
      info_msg("removing Helm release: #{chart}")
      _, ok = capture('helm', 'uninstall', chart, '-n', 'platform', *HELM_KUBE_ARGS)
      if ok
        ok_msg("#{chart} uninstalled")
        removed += 1
      else
        warn_msg("#{chart} (maybe not installed)")
        failures += 1
      end
    end
    puts
    if failures.positive?
      abort "\e[31mProfile '#{profile}' finished with #{failures} failed/skipped step(s) (#{removed} release(s) removed).\e[0m"
    end
    ok_msg("Profile '#{profile}' removed successfully! (#{removed} release(s) uninstalled)")
  end

  # ---------------------------------------------------------------------------
  # Images
  #
  # `kind load docker-image` cannot do this job on Podman: kind drives the
  # ROOTLESS socket while the images are built into the ROOTFUL store, so its
  # presence check answers "not present locally" and it refuses to load. The
  # images are small, local and versionless (poc/*:0.1.0), so they are loaded
  # through containerd directly instead.
  # ---------------------------------------------------------------------------
  POC_IMAGES = %w[
    docker.io/poc/ingest:0.1.0
    docker.io/poc/spark-medallion:0.1.0
    docker.io/poc/flink-stream:0.1.0
    docker.io/poc/lineage-webhook:0.1.0
  ].freeze

  namespace :image do
    desc 'Copy built images into EVERY kind node (args: images=a,b; default: all poc/*)'
    task :load, [:images] do |_t, args|
      banner 'IMAGE LOAD'
      probe = podman_probe
      # EVERY node, not the first one. The scheduler spreads pods across all
      # three kind nodes, and a local image only exists inside the containerd
      # of the node it was imported into: loading into the control-plane alone
      # leaves the workers with nothing, and the pod that lands on a worker
      # fails with
      #   failed to pull image: insufficient_scope: authorization failed
      # which looks exactly like a missing image and is not one.
      nodes = probe[:containers]
      if nodes.empty?
        abort "\e[31mNo kind cluster container found. Run: rake cluster:create\e[0m"
      end
      info_msg("target nodes: #{nodes.map { |n| n[:name] }.join(', ')}")

      images = args[:images].to_s.split(',').map(&:strip).reject(&:empty?)
      images = POC_IMAGES if images.empty?

      # image => [nodes it is present in]
      loaded_into = Hash.new { |h, k| h[k] = [] }
      images.each do |image|
        nodes.each do |node|
          loaded_into[image] << node[:name] if load_image_into(node, image)
        end
      end

      puts
      failed = loaded_into.reject { |image, where| where.size == nodes.size }
      if failed.empty?
        ok_msg("#{images.size} image(s) present in all #{nodes.size} node(s)")
        warn_msg('existing pods keep the digest they started with: recreate them to pick up a new build')
      else
        failed.each do |image, where|
          missing = nodes.map { |n| n[:name] } - where
          err_msg("#{image}: present in #{where.size}/#{nodes.size} node(s), MISSING in #{missing.join(', ')}")
        end
        abort "\e[31m#{failed.size}/#{images.size} image(s) incomplete across the cluster\e[0m"
      end
    end

    desc 'Delete the cached image tars inside the machine (cluster:image:load keeps them)'
    task :clean do
      banner 'IMAGE CLEAN'
      out, ok = capture('podman', 'machine', 'ssh', 'sudo', '-n', 'sh', '-c',
                        "'ls -l /tmp/kind-load-*.tar 2>/dev/null | wc -l; rm -f /tmp/kind-load-*.tar'")
      abort "\e[31mcould not reach the podman machine\e[0m" unless ok

      warn_msg("deleted #{out.to_s.lines.last.to_s.strip} cached tar(s) from the machine")
    end

    desc 'List the images the kind node actually has, with their digests'
    task :list do
      probe = podman_probe
      node = probe[:containers].first
      abort "\e[31mNo kind cluster container found. Run: rake cluster:setup\e[0m" if node.nil?

      banner 'IMAGE LIST'
      run! 'podman', '--connection', node[:connection], 'exec', node[:name],
           'ctr', '-n', 'k8s.io', 'images', 'list'
    end
  end
end

namespace :stream do
  desc 'Deploy Kafka + Flink Operator + the speed-layer jobs via Helm'
  task :deploy do
    puts "\e[33m[S4] Deploying Kafka + Flink Operator + Flink jobs...\e[0m"
    Rake::Task['cluster:up'].invoke('stream')
    puts "\e[32mStream tier deployed. Follow with `rake stream:status` and `rake stream:logs`.\e[0m"
  end

  desc 'Show FlinkDeployment status and the speed-layer pods'
  task :status do
    puts "\e[33m[S4] FlinkDeployment status...\e[0m"
    sh 'kubectl get flinkdeployments -A'
    sh 'kubectl get pods -A -l app=flink-orders-stream'
    sh 'kubectl get pods -A -l app=flink-clickstream-stream'
  end

  desc 'Tail the JobManager logs of the orders speed-layer job'
  task :logs do
    puts "\e[33m[S4] Streaming orders job logs...\e[0m"
    sh 'kubectl logs -n sales -l app=flink-orders-stream --tail=200 -f'
  end

  desc 'Publish domain events into the in-cluster Kafka topics'
  task :produce do
    puts "\e[33m[S4] Publishing events to Kafka for 60 s at 20 events/s...\e[0m"
    sh 'python data-generator/event_producer.py --bootstrap-servers localhost:9092 --rate 20 --duration 60'
  end
end

desc 'RF-24 end-to-end demo on the cluster: code verification -> demo profile -> events -> Flink -> Trino'
task :demo do
  puts '==> Verifying code (the only local step)'
  %w[governance:validate lint typecheck test].each { |t| Rake::Task[t].invoke }

  puts '==> Deploying the demo profile (all 8 subsystems) to the local kind cluster'
  Rake::Task['cluster:up'].invoke('demo')

  puts '==> Waiting for the Flink speed-layer jobs to be RUNNING'
  sh 'kubectl wait --for=condition=ready pod -l app=flink-orders-stream -n sales --timeout=300s' rescue warn_msg('flink orders pod not ready yet')
  sh 'kubectl get flinkdeployments -A'

  puts '==> Publishing events into Kafka for 60 s'
  Rake::Task['stream:produce'].invoke

  puts '==> Waiting one window plus watermark before reading back'
  sleep 15

  puts '==> Querying the speed layer through Trino (batch + stream, RF-11)'
  run! 'kubectl', 'run', 'trino-cli', '--rm', '-i', '--restart=Never', '--image=trinodb/trino:444',
       '--command', '--', 'trino', '--server', 'http://trino.platform.svc.cluster.local:8080',
       '--execute',
       'SELECT status, count(*) AS windows, sum(window_orders) AS orders, sum(window_revenue) AS revenue FROM stream.sales.orders_windowed GROUP BY status ORDER BY status'

  puts '==> Querying the unified serving view (RF-11)'
  run! 'kubectl', 'run', 'trino-unified', '--rm', '-i', '--restart=Never', '--image=trinodb/trino:444',
       '--command', '--', 'trino', '--server', 'http://trino.platform.svc.cluster.local:8080',
       '--execute',
       'SELECT order_date, status, total_orders, total_revenue, source_tier FROM iceberg.serving.sales_unified_revenue ORDER BY order_date DESC LIMIT 20'

  puts
  puts "\e[32mDemo finished. Stream logs: rake stream:logs\e[0m"
end

desc 'Clean local caches and temporary files (test fixtures, bytecode)'
task :clean do
  puts 'Cleaning local caches and bytecode...'
  FileList['data/*.db', 'data/*.jsonl', 'lineage_events.db', '.coverage', '.coverage.*',
           '**/*.pyc', '**/.pytest_cache', '**/__pycache__'].each do |f|
    rm_rf f
    puts "Removed #{f}"
  end
  puts "\e[32mClean complete.\e[0m"
end
