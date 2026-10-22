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
require 'json'
require 'time'
require 'socket'

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
  #
  # /var/tmp, NOT /tmp: in the podman machine /tmp is a tmpfs, so a tar there
  # is RAM. Writing a 1 GB image tar into a 2.9 GB tmpfs took the whole box
  # down (the machine stopped answering the API server) - and the tars pile up,
  # because they are kept as a cache. On the root filesystem they cost disk,
  # of which there is plenty.
  tar = "/var/tmp/kind-load-#{short_id}.tar"

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

# The short id (12 chars) of an image in the machine's own store, or nil.
def local_image_id(image)
  listing, ok = capture('podman', 'images', '--format', '{{.Repository}}:{{.Tag}} {{.Id}}')
  return nil unless ok

  wanted = short_image_ref(image)
  entry = listing.lines.find { |line| short_image_ref(line.split.first.to_s) == wanted }
  entry&.split&.[](1)&.[](0, 12)
end

# Removes the cached tar of an image from the machine.
def drop_image_tar(image)
  short_id = local_image_id(image)
  return if short_id.nil?

  _, ok = capture('podman', 'machine', 'ssh', 'sudo', '-n', 'sh', '-c',
                  "'rm -f /var/tmp/kind-load-#{short_id}.tar'")
  ok ? info_msg("dropped the cached tar of #{image}") : warn_msg("could not drop the tar of #{image}")
end

# Removes the untagged images of a node (the PREVIOUS versions of the images
# that were reloaded) and the content they referenced.
#
# NOT `crictl rmi --prune`: that removes every image no RUNNING container
# uses, including the one that was just loaded and is about to be used by a
# pod that has not started yet (the pod then fails with "pull access denied"
# for an image that is sitting in the node).
def prune_node_images(node)
  script = 'crictl images -o json | python3 -c "import json,sys; ' \
           'd=json.load(sys.stdin); ' \
           'print(\' \'.join(i[\'id\'] for i in d[\'images\'] if not i.get(\'repoTags\')))" ' \
           '| xargs -r crictl rmi'
  _, ok = capture('podman', '--connection', node[:connection], 'exec', node[:name],
                  'sh', '-c', script)
  ok ? info_msg("pruned the untagged images of #{node[:name]}") : warn_msg("could not prune #{node[:name]}")
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

# Is something listening on host:port? Used to wait for a port-forward.
def port_open?(host, port)
  socket = TCPSocket.new(host, port)
  socket.close
  true
rescue StandardError
  false
end

# kind serves a self-signed certificate on 127.0.0.1; every manual command in
# this repo uses this flag, so all Helm invocations go through the same constant.
HELM_KUBE_ARGS = ['--kube-insecure-skip-tls-verify'].freeze

# The Helm releases each startup profile installs (release name == chart dir,
# namespace platform). Mirrors the contract documented in infra/profiles/*.yaml.
#
# One profile = one phase of the platform, and the profiles are EXCLUSIVE
# (RC-02: 8 GB of RAM cannot hold the whole stack at once, so the heavy
# engines run one after the other). Trino is the serving phase on its own:
# Spark (1.8 GiB of limits), Flink (1.8 GiB) and Trino (1.3 GiB) do not fit
# together, and they do not need to: the data they exchange lives in the
# lakehouse (MinIO + Iceberg), not in a pod.
PROFILE_CHARTS = {
  'platform' => %w[s1-ingestion s2-lakehouse s7-fabric],
  'stream'   => %w[s1-ingestion s2-lakehouse s4-stream],
  'batch'    => %w[s1-ingestion s2-lakehouse s3-batch],
  'serving'  => %w[s1-ingestion s2-lakehouse s5-serving],
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

# Helm 4 changed the default apply method: `helm upgrade --server-side`
# defaults to "auto", which means "keep the method the previous revision of
# this release used". Every release in this repo was first installed with
# Helm 4's own default (server-side apply), so every later upgrade keeps
# applying with SSA - and SSA rejects the `helm.sh/hook-*` annotations of a
# hook resource:
#
#   failed to create resource: server-side apply failed for object
#   platform/kafka-topic-creator batch/v1, Kind=Job: failed to create typed
#   patch object (platform/kafka-topic-creator; batch/v1, Kind=Job):
#   .metadata.helm.sh/hook-timeout: field not declared in schema
#
# That killed s2-lakehouse (rev 13, on hook-weight) and s4-stream (rev 4, on
# hook-timeout) and left s2-lakehouse stuck in pending-upgrade. Client-side
# apply computes a strategic merge patch, where removing an annotation is an
# ordinary patch operation instead of an undeclared field, so the hook
# resources deploy again.
HELM_APPLY_ARGS = ['--server-side=false'].freeze

# How long Helm waits for a hook Job. The default is 5 minutes and the
# catalogue bootstrap alone takes 6 to 11 minutes on this box, so the default
# kills the upgrade with "pre-upgrade hooks failed: ... context deadline
# exceeded" while the Job is still doing its work. The per-hook
# `helm.sh/hook-timeout` annotation is the precise control; this is the
# ceiling that applies to the release as a whole.
HELM_TIMEOUT_ARGS = ['--timeout', '20m'].freeze

# Streams `helm upgrade --install` for one chart. Returns true/false.
#
# `sets` are --set overrides. They win over the profile's values file, which
# is what the ramp increments use to switch ONE feature of a chart on or off
# without forking the profile (and without duplicating the RAM budget in a
# second file that would drift).
def helm_upgrade(chart, values_path, sets = {})
  cmd = ['helm', 'upgrade', '--install', chart, "infra/helm/#{chart}",
         '-n', 'platform', *HELM_KUBE_ARGS, *HELM_APPLY_ARGS, *HELM_TIMEOUT_ARGS]
  cmd += ['-f', values_path] if values_path
  sets.each { |key, value| cmd += ['--set', "#{key}=#{value}"] }
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

  desc 'Prove the admission control in the cluster: a DataProduct without owner is rejected'
  task :verify do
    banner 'ADMISSION CONTROL'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    controller, ok = capture('kubectl', 'get', 'pods', '-n', 'kyverno', '--no-headers')
    unless ok
      abort "\e[31mThe Kyverno namespace does not exist. Deploy the governance phase first:\e[0m\n" \
            "\e[33m  rake demo:phases[governance]\e[0m"
    end
    if controller.to_s.strip.empty?
      abort "\e[31mNo Kyverno pod in the kyverno namespace. The controller is what enforces the policies.\e[0m"
    end
    puts controller

    # The DataProduct the cluster must REJECT: it has no owner, and the
    # `require-data-product-owner` ClusterPolicy (Enforce) demands one.
    without_owner = <<~YAML
      apiVersion: dataplatform.io/v1alpha1
      kind: DataProduct
      metadata:
        name: pwned-without-owner
        namespace: sales
      spec:
        description: "A data product with no accountable owner"
        classification: internal
    YAML

    with_owner = <<~YAML
      apiVersion: dataplatform.io/v1alpha1
      kind: DataProduct
      metadata:
        name: allowed-with-owner
        namespace: sales
        owner: data-platform@example.com
      spec:
        description: "A data product with an accountable owner"
        classification: internal
    YAML

    rejected, reject_ok = capture('kubectl', 'apply', '-f', '-', stdin: without_owner)
    accepted, accept_ok = capture('kubectl', 'apply', '-f', '-', stdin: with_owner)

    puts "\n--- without owner (must be rejected) ---"
    puts rejected
    puts "\n--- with owner (must be accepted) ---"
    puts accepted

    problems = []
    problems << 'a DataProduct WITHOUT owner was ACCEPTED: the policy is not enforcing' if reject_ok
    problems << 'a DataProduct WITH owner was rejected: the policy is too strict' unless accept_ok
    if reject_ok && !rejected.to_s.include?('admission webhook')
      problems << "the rejection did not come from an admission webhook: #{rejected.to_s.lines.first}"
    end

    if problems.empty?
      ok_msg('ADMISSION CONTROL OK: the policy is enforced in the cluster')
    else
      problems.each { |problem| err_msg(problem) }
      abort "\e[31mAdmission control is not working as declared.\e[0m"
    end
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
      ok_msg('Cluster is healthy. Detail: kubectl get pods -A · helm history <release> -n platform · rake cluster:logs[<name>]')
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

    # The nodes are started ONE AT A TIME, control plane first.
    #
    # Starting all three at once is what turns a slow disk into an outage:
    # each kubelet pulls, unpacks and mounts its images at the same time, the
    # filesystem (the WSL2 rootfs lives on the Windows filesystem through
    # 9P/virtiofs) saturates, and etcd - which fsyncs on every Raft write -
    # stops answering. The apiserver then times out on every request and the
    # control plane crash-loops:
    #
    #   http: Handler timeout ... rpc error: code = Canceled desc = context canceled
    #   load average: 63.52, 53.10, 32.15   (on 4 CPUs)
    #
    # With the API up first, the workers come up against a working etcd.
    probe[:containers].each do |c|
      if container_running?(c)
        ok_msg("container #{c[:name]} already running (#{c[:connection]})")
        next
      end
      info_msg("starting #{c[:name]} (currently: #{c[:status]}) via #{c[:connection]}...")
      run! 'podman', '--connection', c[:connection], 'start', c[:name]
      ok_msg("container #{c[:name]} started")
      if c[:name].end_with?('-control-plane')
        info_msg('waiting for the API before starting the workers...')
        deadline = Time.now + 180
        last_error = nil
        until api_up?
          if Time.now > deadline
            err_msg('API did not become reachable within 180 s')
            warn_msg("last kubectl error: #{last_error&.lines&.last&.strip}")
            abort "\e[33mInspect the control plane: podman logs #{c[:name]} --tail 50\e[0m"
          end
          last_error, = capture('kubectl', 'get', 'nodes', '--request-timeout=5s')
          sleep 5
        end
        ok_msg('API reachable')
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

  desc 'Collect a full failure report for the cluster, or one pod: describe, events, current and PREVIOUS container logs'
  task :diagnose, [:name] do |_t, args|
    target = args[:name].to_s

    # `kubectl logs` on a restarted container is EMPTY: the terminated container's
    # log lives only in the kubelet's last-termination buffer, reachable with
    # --previous, and it is overwritten by the NEXT restart. Chasing a crash-loop
    # with `cluster:logs` therefore always shows the newest, least informative
    # attempt. This task grabs every window at once, into a file that survives.
    banner 'CLUSTER DIAGNOSE'
    stamp = Time.now.strftime('%Y%m%d-%H%M%S')
    out_dir = File.join('data', 'diagnostics')
    mkdir_p out_dir
    report = File.join(out_dir, "diagnose-#{stamp}.txt")

    pods_out, ok = capture('kubectl', 'get', 'pods', '-A', '--no-headers')
    abort "\e[31mCannot list pods: #{pods_out.lines.first}\e[0m" unless ok

    pods = pods_out.lines.filter_map do |line|
      cols = line.split(/\s+/)
      next nil if cols.size < 5

      ready = cols[2].to_s.split('/', 2)
      {
        ns: cols[0], name: cols[1],
        status: cols[3],
        restarts: cols[4].to_i,
        not_ready: ready[1] && ready[0].to_i < ready[1].to_i
      }
    end

    unhealthy = pods.reject { |p| p[:status].match?(/\A(Running|Succeeded|Completed)\z/) }
                    .reject { |p| p[:restarts].zero? && !p[:not_ready] }
    if target != ''
      unhealthy = pods.select { |p| p[:name].include?(target) }
      if unhealthy.empty?
        abort "\e[31mNo pod matches '#{target}'. List them with: kubectl get pods -A\e[0m"
      end
    end

    File.open(report, 'w') do |f|
      f.puts "POC cluster diagnostic  #{Time.now}"
      f.puts '=' * 78
      f.puts "\n## Pods (#{pods.size} total, #{unhealthy.size} needing attention)\n"
      f.puts format('%-22s %-38s %-24s %s', 'NAMESPACE', 'NAME', 'STATUS', 'RESTARTS')
      pods.sort_by { |p| [p[:status] == 'Running' ? 1 : 0, p[:ns], p[:name]] }.each do |p|
        f.puts format('%-22s %-38s %-24s %d', p[:ns], p[:name], p[:status], p[:restarts])
      end

      # Jobs are where this platform actually fails (bootstrap/hook Jobs), and
      # a failed Job's pod is often already gone: its state lives in .status.
      jobs_out, jobs_ok = capture('kubectl', 'get', 'jobs', '-A', '--no-headers')
      if jobs_ok && !jobs_out.strip.empty?
        f.puts "\n## Jobs\n"
        f.puts format('%-22s %-38s %-8s %-8s %-6s %s', 'NAMESPACE', 'NAME', 'COMPLETIONS', 'FAILED', 'ACTIVE', 'AGE')
        jobs_out.lines.each do |line|
          cols = line.split(/\s+/)
          next if cols.size < 6

          f.puts format('%-22s %-38s %-8s %-8s %-6s %s', cols[0], cols[1], cols[2], cols[3], cols[4], cols[5])
        end
      end

      # Custom resources: the Flink/Spark operators report job-level failures
      # here, not on any pod, so a pod-only sweep misses them entirely.
      { 'flinkdeployments' => 'FlinkDeployments',
        'scheduledsparkapplications' => 'ScheduledSparkApplications',
        'sparkapplications' => 'SparkApplications' }.each do |plural, title|
        cr_out, cr_ok = capture('kubectl', 'get', plural, '-A', '--no-headers')
        next unless cr_ok && !cr_out.strip.empty?

        f.puts "\n## #{title}\n#{cr_out}"
      end

      focus = unhealthy.empty? ? pods : unhealthy
      focus.each do |p|
        ident = "#{p[:ns]}/#{p[:name]}"
        f.puts "\n#{'=' * 78}\n## #{ident}  [#{p[:status]}, #{p[:restarts]} restarts]\n#{'=' * 78}"

        %w[describe].each do |what|
          out, = capture('kubectl', what, '-n', p[:ns], p[:name])
          f.puts "\n--- kubectl #{what} ---\n#{out}"
        end
        events, = capture('kubectl', 'get', 'events', '-n', p[:ns],
                           '--field-selector', "involvedObject.name=#{p[:name]}",
                           '--sort-by', 'lastTimestamp')
        f.puts "\n--- events ---\n#{events}"

        containers_out, c_ok = capture('kubectl', 'get', 'pod', '-n', p[:ns], p[:name],
                                       '-o', 'jsonpath={.spec.initContainers[*].name}{"\\n"}{.spec.containers[*].name}')
        containers = c_ok ? containers_out.split : []
        containers.each do |c|
          # --previous is the only source for a container that already died.
          prev, = capture('kubectl', 'logs', '-n', p[:ns], p[:name], '-c', c, '--previous', '--tail=200')
          cur, = capture('kubectl', 'logs', '-n', p[:ns], p[:name], '-c', c, '--tail=200')
          f.puts "\n--- logs #{c} (PREVIOUS container) ---\n#{prev}" unless prev.strip.empty?
          f.puts "\n--- logs #{c} (current) ---\n#{cur}"
        end
      end
    end

    if unhealthy.empty?
      ok_msg('every pod is Running/Completed with zero restarts')
    else
      warn_msg("#{unhealthy.size} pod(s) need attention:")
      unhealthy.each { |p| warn_msg("  #{p[:ns]}/#{p[:name]}  [#{p[:status]}, #{p[:restarts]} restarts]") }
    end
    ok_msg("report written to #{report}")
    puts "        read it with:  rake cluster:logs[<pod>]  or open the file above"
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

  desc "Deploy a startup profile with Helm (args: profile=platform|stream|batch|serving|demo, default: stream)"
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

  desc 'Seed the OLTP database with the synthetic demo dataset (args: records=105)'
  task :seed, [:records] do |_t, args|
    banner 'OLTP SEED'
    records = (args[:records] || 105).to_i

    # The schema is created by the postgres init script, but the DATA is not:
    # the chart has no seeding step, so without this the medallion reads an
    # empty OLTP source and reports EMPTY for every entity (which is what it
    # did before this task existed).
    #
    # The seeder runs on the HOST, because it needs Faker and numpy and the
    # Postgres service is a ClusterIP: it reaches the database through a
    # port-forward that this task opens and closes.
    values = YAML.load_file(File.join('infra', 'helm', 's1-ingestion', 'values.yaml'))
    env = values.dig('ingestionRunner', 'env') || {}
    password = env['PGPASSWORD']
    database = env['PGDATABASE'] || 'poc_oltp'
    user = env['PGUSER'] || 'postgres'
    abort "\e[31mno PGPASSWORD in infra/helm/s1-ingestion/values.yaml\e[0m" if password.nil?

    %w[faker numpy psycopg2 click].each do |mod|
      _, ok = capture('python', '-c', "import #{mod}")
      unless ok
        abort "\e[31mThe host python is missing '#{mod}'. The seeder needs: " \
              "pip install faker numpy psycopg2-binary click\e[0m"
      end
    end

    port_forward = Process.spawn('kubectl', 'port-forward', '-n', 'platform',
                                 'svc/postgres-oltp', '5432:5432',
                                 out: File::NULL, err: File::NULL)
    begin
      ready = false
      30.times do
        if port_open?('127.0.0.1', 5432)
          ready = true
          break
        end
        sleep 1
      end
      abort "\e[31mthe port-forward to postgres-oltp did not open\e[0m" unless ready

      run! 'python', 'data-generator/oltp_seed.py',
           '--host', '127.0.0.1', '--port', '5432', '--db', database,
           '--user', user, '--password', password, '--records', records.to_s
    ensure
      begin
        Process.kill('TERM', port_forward)
        Process.wait(port_forward)
      rescue StandardError
        nil
      end
    end
    ok_msg("OLTP seeded: #{records} orders (+ products, customers, campaigns)")
  end

  desc "Tear a startup profile down (args: profile=platform|stream|batch|serving|demo, default: stream)"
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

  # How each local image is built: tag => [dockerfile, build context].
  # The context is the directory the COPY paths are relative to: the repo
  # root for the three images that carry the repository code, and
  # lineage/config for the webhook, which is self-contained.
  POC_IMAGE_BUILDS = {
    'docker.io/poc/ingest:0.1.0'         => ['infra/docker/ingest.Dockerfile', '.'],
    'docker.io/poc/spark-medallion:0.1.0' => ['infra/docker/spark.Dockerfile', '.'],
    'docker.io/poc/flink-stream:0.1.0'   => ['infra/docker/flink.Dockerfile', '.'],
    'docker.io/poc/lineage-webhook:0.1.0' => ['lineage/config/Dockerfile', 'lineage/config']
  }.freeze

  namespace :image do
    desc 'Build the poc/* images with podman and load them into every node (args: images=a,b; default: all)'
    task :build, [:images] do |_t, args|
      banner 'IMAGE BUILD'
      images = args[:images].to_s.split(/[,+]/).map(&:strip).reject(&:empty?)
      images = POC_IMAGE_BUILDS.keys if images.empty?

      failures = 0
      built = []
      images.each do |image|
        recipe = POC_IMAGE_BUILDS[image]
        if recipe.nil?
          err_msg("no build recipe for #{image}")
          failures += 1
          next
        end
        dockerfile, context = recipe
        unless File.exist?(dockerfile)
          err_msg("missing #{dockerfile}")
          failures += 1
          next
        end
        info_msg("building #{image} from #{dockerfile} (context: #{context})")
        if system('podman', 'build', '-f', dockerfile, '-t', image, context)
          ok_msg(image)
          built << image
        else
          err_msg(image)
          failures += 1
        end
      end

      if failures.positive?
        abort "\e[31m#{failures}/#{images.size} image(s) failed to build.\e[0m"
      end
      ok_msg("#{built.size} image(s) built")

      # The build layers of a rebuilt image are dangling: they are the biggest
      # consumer of the machine's disk and nothing else removes them.
      capture('podman', 'image', 'prune', '-f')

      # RNF-03 asks for ONE versioned procedure with no manual step, so the
      # build does not stop at the image: it also puts it where the cluster
      # can use it. `cluster:image:load` on its own still works.
      Rake::Task['cluster:image:load'].invoke(built.join('+'))
    end

    desc 'Copy built images into EVERY kind node (args: images=a,b or a+b; default: all poc/*)'
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

      images = args[:images].to_s.split(/[,+]/).map(&:strip).reject(&:empty?)
      images = POC_IMAGES if images.empty?

      # image => [nodes it is present in]
      loaded_into = Hash.new { |h, image| h[image] = [] }
      images.each do |image|
        nodes.each do |node|
          loaded_into[image] << node[:name] if load_image_into(node, image)
        end

        # The tar was a cache so that a failed import would not pay for the
        # save again. Once the image is in every node it is dead weight on
        # the machine's disk (a Spark image is ~950 MB), and so is the
        # PREVIOUS version of the image in each node's containerd store:
        # four rebuilds left ~10 GB of dead blobs per node, which is what
        # filled the host disk.
        next unless loaded_into[image].size == nodes.size

        drop_image_tar(image)
        nodes.each { |node| prune_node_images(node) }
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

    desc 'Delete the cached image tars inside the machine and prune the node image stores'
    task :clean do
      banner 'IMAGE CLEAN'
      out, ok = capture('podman', 'machine', 'ssh', 'sudo', '-n', 'sh', '-c',
                        "'rm -f /var/tmp/kind-load-*.tar'")
      abort "\e[31mcould not reach the podman machine\e[0m" unless ok

      # Every rebuild of an image left its PREVIOUS version in the node's
      # containerd store, untagged, and nothing ever removed it: four rebuilds
      # of the Spark image left ~10 GB of dead blobs per node, and the host
      # disk (the machine's VHDX) filled up. The untagged images are pruned
      # after every load, and this task prunes whatever is left.
      probe = podman_probe
      probe[:containers].each { |node| prune_node_images(node) }

      warn_msg('deleted the cached image tars and pruned the node image stores')
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

# The Trino image, read from the chart so the demo and the deployment cannot
# drift apart. A hardcoded tag here is how the serving layer ended up running
# a Trino that could not talk to Polaris at all.
def trino_image
  doc = YAML.load_file(File.join('infra', 'helm', 's5-serving', 'values.yaml'))
  image = doc.dig('trino', 'image')
  "#{image['repository']}:#{image['tag']}"
rescue StandardError
  'trinodb/trino:455'
end

# ------------------------------------------------------------------------------
# The windowed speed-layer table cannot be counted with a query engine here:
#
#   * PyIceberg refuses the table ("does not yet support equality deletes"),
#     because the Flink sink writes in upsert mode;
#   * Trino cannot be up AT THE SAME TIME as Flink on a 6 GiB box, which is the
#     whole reason the increments exist.
#
# So the state is read from the table metadata in MinIO: the current
# snapshot's summary carries total-records and total-data-files.
# ------------------------------------------------------------------------------
def orders_windowed_state(table = 'orders_windowed', namespace = 'sales')
  user, = capture('kubectl', 'get', 'deploy', 'minio', '-n', 'platform', '-o',
                  'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="MINIO_ROOT_USER")].value}')
  password, = capture('kubectl', 'get', 'deploy', 'minio', '-n', 'platform', '-o',
                      'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="MINIO_ROOT_PASSWORD")].value}')
  user = 'minioadmin' if user.to_s.strip.empty?
  password = 'minioadmin' if password.to_s.strip.empty?

  script = <<~SH
    export MC_CONFIG_DIR=/tmp/.mc
    mc alias set local http://localhost:9000 #{user.to_s.strip} #{password.to_s.strip} >/dev/null 2>&1
    latest=$(mc ls local/warehouse/#{namespace}/#{table}/metadata/ | awk '{print $NF}' | grep 'metadata.json' | sort | tail -1)
    test -n "$latest" || { echo NO_METADATA; exit 1; }
    mc cat "local/warehouse/#{namespace}/#{table}/metadata/$latest"
  SH

  out, ok = capture('kubectl', 'exec', '-n', 'platform', 'deploy/minio', '--', 'sh', '-c', script)
  body = out.to_s[out.to_s.index('{')..] if out.to_s.include?('{')
  unless ok && body
    abort "\e[31mNo se pudo leer la metadata de #{namespace}.#{table} en MinIO: #{out.to_s.strip}\e[0m"
  end
  JSON.parse(body)
end

def snapshot_records(state)
  snapshot = state['snapshots'].find { |s| s['snapshot-id'] == state['current-snapshot-id'] } ||
             state['snapshots'].last
  snapshot.dig('summary', 'total-records').to_s
end

# Every line the Iceberg committer writes when it closes a checkpoint:
#   2026-10-22 15:23:15,603 INFO ... Committed rowDelta to table: ..., checkpointId 2 in 13265 ms
def commit_timestamps
  out, ok = capture('kubectl', 'logs', '-n', 'sales', '-l', 'component=taskmanager', '--tail=1000')
  return [] unless ok

  out.lines.filter_map do |line|
    match = line.match(/^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[,.]\d{3}).*Committed rowDelta.*checkpointId (\d+) in (\d+) ms/)
    next unless match

    { at: Time.parse(match[1].tr(',', '.')), checkpoint: match[2], duration_ms: match[3].to_i }
  end
end

def last_commit_timestamp
  commits = commit_timestamps
  commits.last&.dig(:at)
end

# The FlinkDeployment as a Hash, or nil when it is not deployed.
def flink_state(deployment, namespace)
  out, ok = capture('kubectl', 'get', 'flinkdeployment', deployment, '-n', namespace, '-o', 'json')
  ok ? JSON.parse(out) : nil
end

# Rotates the Kafka consumer group and rewinds to offset 0 on the live
# FlinkDeployment. The env lives in the pod templates (the operator puts it
# there), not in spec.env, and main() runs in the JobManager pod, so BOTH
# templates are patched.
#
# Same rule as pipelines/orchestration/flink_replay.py: a group that has
# already committed offsets ignores KAFKA_START_OFFSET=earliest, so rotating
# the group is what actually makes the replay happen.
def rotate_consumer_group!(deployment, namespace, state)
  abort "\e[31m#{deployment} is not deployed.\e[0m" if state.nil?

  containers = %w[jobManager taskManager].flat_map do |component|
    Array(state.dig('spec', component, 'podTemplate', 'spec', 'containers'))
      .each_with_index
      .map { |container, index| [component, index, Array(container['env'])] }
  end
  abort "\e[31m#{deployment} declares no pod-template env.\e[0m" if containers.empty?

  group_var = containers.flat_map { |_, _, env| env.map { |entry| entry['name'] } }
                        .find { |name| %w[KAFKA_GROUP_ID KAFKA_CLICKSTREAM_GROUP_ID].include?(name) }
  unless group_var
    abort "\e[31mNo consumer-group variable in the pod templates; a replay cannot be guaranteed.\e[0m"
  end

  group = "#{deployment}-replay-#{Time.now.utc.strftime('%Y%m%d%H%M%S')}"
  operations = []
  containers.each do |component, container_index, env|
    env.each_with_index do |entry, env_index|
      value = case entry['name']
              when group_var then group
              when 'KAFKA_START_OFFSET' then 'earliest'
              end
      next unless value

      operations << {
        'op' => 'replace',
        'path' => "/spec/#{component}/podTemplate/spec/containers/#{container_index}/env/#{env_index}/value",
        'value' => value
      }
    end
  end
  abort "\e[31mNo env var to patch (#{group_var} / KAFKA_START_OFFSET).\e[0m" if operations.empty?

  run! 'kubectl', 'patch', 'flinkdeployment', deployment, '-n', namespace,
       '--type', 'json', '-p', JSON.generate(operations)
  [group_var, group]
end

# Waits for a commit NEWER than the one seen before the patch: that is the
# proof the job came back up and flushed the replayed windows.
def wait_for_new_commit(deployment, namespace, previous_commit, timeout_minutes: 20)
  info_msg('esperando a que el job relea el log y cierre un checkpoint...')
  deadline = Time.now + (timeout_minutes * 60)
  loop do
    current = last_commit_timestamp
    return current if current && previous_commit && current > previous_commit
    if Time.now > deadline
      state, = capture('kubectl', 'get', 'flinkdeployment', deployment, '-n', namespace,
                       '-o', 'jsonpath={.status.jobStatus.state}')
      abort "\e[31mNo hubo un commit nuevo en #{timeout_minutes} min (job=#{state.to_s.strip}).\e[0m"
    end
    sleep 15
  end
end

def suspend_flink!(deployment, namespace)
  run! 'kubectl', 'patch', 'flinkdeployment', deployment, '-n', namespace,
       '--type', 'merge', '-p', JSON.generate('spec' => { 'job' => { 'state' => 'suspended' } })
  deadline = Time.now + (5 * 60)
  loop do
    pods, = capture('kubectl', 'get', 'pods', '-n', namespace, '-l', "app=#{deployment}",
                    '-o', 'jsonpath={.items[*].metadata.name}')
    return if pods.to_s.strip.empty?
    abort "\e[31mlos pods de #{deployment} no se fueron en 5 min.\e[0m" if Time.now > deadline

    sleep 5
  end
end

def resume_flink!(deployment, namespace)
  run! 'kubectl', 'patch', 'flinkdeployment', deployment, '-n', namespace,
       '--type', 'merge', '-p', JSON.generate('spec' => { 'job' => { 'state' => 'running' } })
end

def serving_up
  run! 'kubectl', 'scale', 'deploy/trino', '-n', 'platform', '--replicas=1'
  wait_for_workload('platform', 'deploy/trino')
end

# One line out of the Trino CLI: the digest both reads have to agree on.
#
# The Deployment reports Ready before the server accepts queries ("Trino
# server is still initializing"), so the read is retried for a few minutes.
def trino_digest(sql, timeout_minutes: 6)
  deadline = Time.now + (timeout_minutes * 60)
  loop do
    out, ok = capture('kubectl', 'run', 'trino-digest', '-n', 'platform', '--rm', '-i',
                      '--restart=Never', '--image=' + trino_image,
                      '--image-pull-policy=IfNotPresent', '--command', '--',
                      'trino', '--server', 'http://trino.platform.svc.cluster.local:8080',
                      '--output-format', 'TSV', '--execute', sql)
    # kubectl prints "pod ... deleted from platform namespace" AFTER the query
    # output, so the answer is picked by its shape (TSV row starting with the
    # row count) and not by position.
    line = out.lines.map(&:strip).find { |candidate| candidate.match?(/^\d+\t/) }
    return line if ok && line
    unless out.to_s.include?('still initializing')
      warn_msg("la consulta a Trino fallo: #{out.to_s.strip}") unless ok
      return line
    end

    return nil if Time.now > deadline

    sleep 20
  end
end

namespace :stream do
  desc 'Show the Iceberg state of the windowed table (reads the metadata, no query engine)'
  task :state do
    banner 'STREAM TABLE STATE'
    state = orders_windowed_state
    snapshot = state['snapshots'].find { |s| s['snapshot-id'] == state['current-snapshot-id'] } ||
               state['snapshots'].last
    summary = snapshot&.fetch('summary', {}) || {}
    puts "  table            sales.orders_windowed"
    puts "  current snapshot #{state['current-snapshot-id']}"
    puts "  operation        #{summary['operation']}"
    puts "  total-records    #{summary['total-records']}"
    puts "  total-data-files #{summary['total-data-files']}"
    puts "  snapshots        #{state['snapshots'].length}"
  end

  desc 'RF-10: replay the whole log into the speed layer (rotates the consumer group)'
  task :replay do
    banner 'STREAM REPLAY (RF-10)'
    deployment = 'flink-orders-stream'
    namespace = 'sales'

    state = flink_state(deployment, namespace)
    abort "\e[31m#{deployment} is not deployed. Bring the speed layer up first: rake ramp:stream\e[0m" if state.nil?

    last_commit = last_commit_timestamp
    group_var, group = rotate_consumer_group!(deployment, namespace, state)
    info_msg("#{group_var} -> #{group} y KAFKA_START_OFFSET=earliest")
    wait_for_new_commit(deployment, namespace, last_commit)
    ok_msg('el job releyo el log entero desde el offset 0 y volvio a commitear')
    info_msg('nota: total-records CRECE aunque el resultado sea identico, porque el')
    info_msg('sink escribe en UPSERT (merge-on-read): cada commit anade los registros')
    info_msg('nuevos y los equality-deletes de los anteriores. La prueba logica:')
    info_msg('  rake stream:replay:verify')
  end

  desc 'RF-10 acceptance: a full replay regenerates the SAME logical result (suspends Flink, uses Trino)'
  task 'replay:verify' do
    banner 'STREAM REPLAY - VERIFICACION LOGICA (RF-10)'
    deployment = 'flink-orders-stream'
    namespace = 'sales'
    sql = <<~SQL.gsub(/\s+/, ' ').strip
      SELECT count(*) AS filas,
             CAST(round(sum(window_orders)) AS bigint) AS pedidos,
             CAST(round(sum(window_revenue), 2) AS varchar) AS ingresos,
             to_hex(md5(to_utf8(array_join(array_agg(
               concat(CAST(window_start AS varchar), '|', status, '|',
                      CAST(window_orders AS varchar), '|',
                      CAST(round(window_revenue, 2) AS varchar)) ORDER BY 1), ',')))) AS huella
      FROM iceberg.sales.orders_windowed
    SQL

    state = flink_state(deployment, namespace)
    abort "\e[31m#{deployment} is not deployed. Bring the speed layer up first: rake ramp:stream\e[0m" if state.nil?

    # Flink and Trino do not fit in this box AT THE SAME TIME (that is what the
    # increments exist for), so the two reads are taken with the job suspended:
    # upgradeMode=stateless makes suspending cheap and safe.
    info_msg('suspendiendo el job para dejar sitio a Trino...')
    suspend_flink!(deployment, namespace)
    serving_up
    before = trino_digest(sql)
    apply_down([['scale', 'platform', 'deploy/trino', 0]])
    puts "  antes del replay  #{before}"

    if before.nil? || before.to_s.strip.empty?
      abort "\e[31mTrino no devolvio nada; no se puede comparar.\e[0m"
    end

    last_commit = last_commit_timestamp
    resume_flink!(deployment, namespace)
    group_var, group = rotate_consumer_group!(deployment, namespace,
                                              flink_state(deployment, namespace))
    info_msg("#{group_var} -> #{group} (segundo replay completo del mismo log)")
    wait_for_new_commit(deployment, namespace, last_commit)

    info_msg('suspendiendo otra vez para volver a leer con Trino...')
    suspend_flink!(deployment, namespace)
    serving_up
    after = trino_digest(sql)
    puts "  despues del replay #{after}"

    if before == after
      ok_msg('RF-10 VERIFICADO: releer el log entero regenera exactamente el mismo')
      ok_msg('resultado logico (misma huella md5); el replay es idempotente.')
    else
      abort "\e[31mRF-10: el replay cambio el resultado logico.\n  antes:   #{before}\n" \
            "  despues: #{after}\e[0m"
    end
  end

  desc 'RF-09: measured latency from the producer log to the Iceberg commit'
  task :latency do
    banner 'STREAM LATENCY (RF-09)'
    producer, ok = capture('kubectl', 'logs', '-n', 'platform', 'job/event-producer')
    abort "\e[31mNo event-producer Job to read. Run: rake stream:produce:cluster\e[0m" unless ok

    finished = producer.lines.grep(/Finished publishing/).last
    abort "\e[31mThe producer log has no 'Finished publishing' line.\e[0m" if finished.nil?
    produced_at = Time.parse(finished[/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d/].tr('T', ' '))
    events = finished[/(\d+) events/, 1]

    commits = commit_timestamps
    if commits.empty?
      abort "\e[31mNo commits in the TaskManager log. Is the speed layer RUNNING? rake stream:logs\e[0m"
    end

    # A commit that is logged just after the producer stopped may have STARTED
    # before the last event was produced (the log line carries the END of the
    # commit, which on this box takes 8-17 s), so the honest measurement waits
    # for the sink to go quiet: once no new commit appears for 45 s there is
    # nothing left to flush and the last commit carried the final windows.
    info_msg('esperando a que el sink se quede quieto (45 s sin commits)...')
    quiet_deadline = Time.now + (5 * 60)
    last_seen = commits.last[:at]
    loop do
      sleep 15
      current = commit_timestamps
      if current.length > commits.length
        commits = current
        last_seen = commits.last[:at]
      end
      break if Time.now - last_seen > 45
      break if Time.now > quiet_deadline
    end

    after = commits.select { |commit| commit[:at] >= produced_at }

    puts "  productor termino   #{produced_at}  (#{events} eventos)"
    puts "  commits observados  #{commits.length}"
    puts
    puts '  ultimos commits (checkpoint -> duracion -> hora):'
    commits.last(5).each do |commit|
      puts format('    #%-4s %6s ms  %s', commit[:checkpoint], commit[:duration_ms], commit[:at])
    end
    puts

    if after.empty?
      warn_msg('ningun commit posterior al productor: la ventana aun no se ha cerrado')
      return
    end

    best = (after.first[:at] - produced_at).round(1)
    worst = (after.last[:at] - produced_at).round(1)
    puts "  ultimo evento -> primer commit:  #{best} s"
    puts "  ultimo evento -> commit final:   #{worst} s   (ya no quedaban filas por escribir)"
    puts "  duracion del commit final:       #{after.last[:duration_ms]} ms"
    puts
    puts '  desglose del presupuesto:'
    puts '    ventana + watermark (config):        <= 4.5 s'
    puts '    espera al siguiente checkpoint:      <= 30 s (enable_checkpointing(30_000))'
    puts "    commit del sink (medido):            ~ #{after.last[:duration_ms]} ms (I/O de esta maquina)"
    puts
    info_msg('RF-09: el agregado se CALCULA en <= 4.5 s, pero la FILA queda visible')
    info_msg("solo en el siguiente checkpoint: #{worst} s medidos de punta a punta aqui.")
  end

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

  desc 'Publish domain events into Kafka FROM INSIDE the cluster (args: rate=20 duration=60)'
  task 'produce:cluster', [:rate, :duration] do |_t, args|
    banner 'STREAM PRODUCE (in-cluster)'
    job_file = File.join('infra', 'diagnostics', 'event-producer.job.yaml')
    abort "\e[31mMissing #{job_file}\e[0m" unless File.exist?(job_file)

    rate = args[:rate].to_s.strip
    rate = '20' if rate.empty?
    duration = args[:duration].to_s.strip
    duration = '60' if duration.empty?

    _, api_ok = capture('kubectl', 'cluster-info')
    unless api_ok
      abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m"
    end
    brokers, = capture('kubectl', 'get', 'endpoints', 'kafka', '-n', 'platform',
                       '-o', 'jsonpath={.subsets[0].addresses[0].ip}')
    if brokers.to_s.strip.empty?
      abort "\e[31mThe Kafka broker is not ready. Run: rake ramp:kafka\e[0m"
    end

    # The rate and the duration are part of the container's command, and a
    # Job's spec is immutable once its pod runs, so the manifest is submitted
    # with the values already in it.
    #
    # The check is on the RESULT and not on "the text changed": with the
    # default rate and duration (20 and 60) the substitution is a no-op and
    # comparing against the original file reported a bogus patch failure.
    manifest = File.read(job_file)
                   .sub('- "20"', "- \"#{rate}\"")
                   .sub('- "60"', "- \"#{duration}\"")
    unless manifest.include?("- \"#{rate}\"") && manifest.include?("- \"#{duration}\"")
      abort "\e[31mcould not patch the rate/duration into the manifest.\e[0m"
    end

    run! 'kubectl', 'delete', 'job', 'event-producer', '-n', 'platform', '--ignore-not-found'
    applied, apply_ok = capture('kubectl', 'apply', '-f', '-', stdin: manifest)
    abort "\e[31mno se pudo crear el Job del producer: #{applied}\e[0m" unless apply_ok

    info_msg("publicando #{rate} eventos/s durante #{duration}s dentro del clúster...")

    deadline = Time.now + (duration.to_i + 180)
    phase = ''
    loop do
      complete, = capture('kubectl', 'get', 'job', 'event-producer', '-n', 'platform',
                          '-o', 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')
      if complete.to_s.strip == 'True'
        phase = 'complete'
        break
      end
      failed, = capture('kubectl', 'get', 'job', 'event-producer', '-n', 'platform',
                        '-o', 'jsonpath={.status.failed}')
      if failed.to_s.strip.to_i.positive?
        phase = 'failed'
        break
      end
      if Time.now > deadline
        phase = 'timeout'
        break
      end
      sleep 10
    end

    puts
    logs, = capture('kubectl', 'logs', '-n', 'platform', 'job/event-producer')
    puts logs unless logs.to_s.strip.empty?
    puts

    case phase
    when 'complete'
      ok_msg('EVENTOS PUBLICADOS EN KAFKA')
    when 'failed'
      abort "\e[31mPRODUCE: el Job fallo (exit != 0). Lee el log de arriba.\e[0m"
    else
      abort "\e[31mPRODUCE: no termino a tiempo. Logs: kubectl logs -n platform job/event-producer\e[0m"
    end
  end

  desc 'Prove the speed layer in ONE pod: window operators -> Iceberg -> Parquet (no Kafka/JM/TM)'
  task :probe do
    banner 'SPEED LAYER PROBE'
    script = File.join('infra', 'diagnostics', 'window_write_probe.py')
    job_file = File.join('infra', 'diagnostics', 'window-write-probe.job.yaml')
    abort "\e[31mMissing #{script}\e[0m" unless File.exist?(script)
    abort "\e[31mMissing #{job_file}\e[0m" unless File.exist?(job_file)

    _, api_ok = capture('kubectl', 'cluster-info')
    unless api_ok
      abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m"
    end
    # MinIO + Polaris must be up: the probe writes through them.
    up, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                  '-o', 'jsonpath={.items[*].metadata.name}')
    %w[minio polaris].each do |component|
      next if up.to_s.match?(/\A#{component}-\d/) || up.to_s.match?(/#{component}-[a-z0-9]+-/)

      abort "\e[31m#{component} is not running in namespace platform.\e[0m\n" \
            "\e[33mDeploy it first: rake cluster:up[stream]\e[0m"
    end

    # The script goes in through a ConfigMap so it can be corrected without
    # paying the image rebuild + per-node load cycle (~20 min here).
    run! 'kubectl', 'delete', 'job', 'flink-window-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'delete', 'configmap', 'flink-window-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'create', 'configmap', 'flink-window-probe', '-n', 'platform',
         "--from-file=window_write_probe.py=#{script}"

    run! 'kubectl', 'apply', '-f', job_file
    info_msg('probe running; this is a full local Flink cluster in one pod, allow ~3 min...')

    deadline = Time.now + (12 * 60)
    phase = ''
    loop do
      complete, = capture('kubectl', 'get', 'job', 'flink-window-probe', '-n', 'platform',
                          '-o', 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')
      if complete.to_s.strip == 'True'
        phase = 'complete'
        break
      end
      failed, = capture('kubectl', 'get', 'job', 'flink-window-probe', '-n', 'platform',
                        '-o', 'jsonpath={.status.failed}')
      if failed.to_s.strip.to_i.positive?
        phase = 'failed'
        break
      end
      if Time.now > deadline
        phase = 'timeout'
        break
      end
      sleep 10
    end

    puts
    logs, = capture('kubectl', 'logs', '-n', 'platform', 'job/flink-window-probe')
    puts logs unless logs.to_s.strip.empty?
    puts

    passes = logs.to_s.scan(/PASS\s+(.*)/).flatten
    fails = logs.to_s.scan(/FAIL\s+(.*)/).flatten
    ok_msg("#{passes.size} comprobacion(es) superadas") unless passes.empty?
    fails.each { |f| err_msg("comprobacion fallida: #{f}") }

    # Independent of the Job's own verdict: did Parquet actually land? Asked of
    # the MinIO pod, not the probe pod - the probe's container has exited by now,
    # so exec would fail with "container not found" no matter what it wrote.
    minio_pod, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                         '-o', 'jsonpath={.items[?(@.metadata.labels.app=="minio")].metadata.name}')
    minio_pod = minio_pod.to_s.strip
    if minio_pod.empty?
      err_msg('no se ha encontrado el pod de MinIO para contar los parquet')
    else
      data, = capture('kubectl', 'exec', '-n', 'platform', minio_pod, '--',
                      'sh', '-c',
                      "find /data/warehouse/sales/probe_orders_windowed/data -name '*.parquet' | wc -l")
      count = data.to_s.strip
      if count =~ /\A\d+\z/ && count.to_i.positive?
        ok_msg("MinIO contiene #{count} fichero(s) parquet en sales.probe_orders_windowed")
      else
        err_msg("no se ha encontrado ningun parquet en el warehouse (#{data.to_s.lines.first})")
      end
    end

    case phase
    when 'complete'
      fails.empty? ? ok_msg('PROBE OK') : abort("\e[31mPROBE FALLIDO: #{fails.join('; ')}\e[0m")
    when 'failed'
      abort "\e[31mPROBE: el Job fallo (exit != 0). Lee el log de arriba.\e[0m"
    else
      abort "\e[31mPROBE: no termino en 12 min. Logs: kubectl logs -n platform job/flink-window-probe\e[0m"
    end
    puts "\e[32mLa speed layer escribe Parquet. NO cubre Kafka, el FlinkDeployment ni los checkpoints.\e[0m"
  end
end

# ------------------------------------------------------------------------------
# Batch tier: run the medallion on demand
# ------------------------------------------------------------------------------
#
# The medallion has TWO entry points per tier, and they are the same code:
#
#   bronze  the s1 CronJob (poc/ingest:0.1.0, plain Python) - the one path
#           that writes bronze; the Spark bronze job is disabled in s3-batch
#           so the two never write the same Iceberg table.
#   silver  the s3 ScheduledSparkApplication (spark-silver-transform).
#   gold    the s3 ScheduledSparkApplication (spark-gold-aggregation).
#
# The crons fire at 02:00/03:00/04:00, which is no use for a demo or for a
# verification run, so this task submits the SAME spec on demand:
#
#   * bronze: the CronJob is suspended and a one-shot Job is created from it
#     (`kubectl create job --from=cronjob/`), so what runs is exactly what
#     the cron would have run.
#   * silver/gold: the spec of the deployed ScheduledSparkApplication is read
#     back and applied as a plain SparkApplication. The chart stays the only
#     source of truth for what a tier runs, so the manual run and the
#     scheduled run cannot drift.
# ------------------------------------------------------------------------------
BATCH_TIERS = {
  'bronze' => { kind: 'cronjob', object: 'medallion-bronze-ingestion-cron', ns: 'platform' },
  'silver' => { kind: 'spark', object: 'spark-silver-transform', ns: 'sales' },
  'gold'   => { kind: 'spark', object: 'spark-gold-aggregation', ns: 'sales' }
}.freeze

def wait_for_spark_application(name, ns, timeout_s)
  deadline = Time.now + timeout_s
  loop do
    state, = capture('kubectl', 'get', 'sparkapplication', name, '-n', ns,
                      '-o', 'jsonpath={.status.applicationState.state}')
    case state.to_s.strip
    when 'COMPLETED'
      return true
    when 'FAILED', 'UNKNOWN', 'SUBMISSION_FAILED', 'INVALIDATING'
      return false
    end
    return false if Time.now > deadline

    sleep 10
  end
end

# Applies the spec of a ScheduledSparkApplication as a one-shot
# SparkApplication. Returns the name it created.
def submit_one_shot_spark(scheduled_name, ns)
  raw, ok = capture('kubectl', 'get', 'scheduledsparkapplication', scheduled_name,
                    '-n', ns, '-o', 'jsonpath={.spec.template}')
  unless ok
    abort "\e[31mno se pudo leer el ScheduledSparkApplication #{scheduled_name} " \
          "(is s3-batch deployed?)\e[0m"
  end

  manifest = {
    'apiVersion' => 'sparkoperator.k8s.io/v1beta2',
    'kind' => 'SparkApplication',
    'metadata' => { 'name' => "#{scheduled_name}-oneshot", 'namespace' => ns },
    'spec' => JSON.parse(raw)
  }
  file = Tempfile.new(['spark-oneshot-', '.json'])
  file.write(JSON.pretty_generate(manifest))
  file.close

  run! 'kubectl', 'delete', 'sparkapplication', "#{scheduled_name}-oneshot",
       '-n', ns, '--ignore-not-found'
  # A failed attempt leaves the driver pod behind (the operator creates it
  # and the pod stays in Failed/StartError), and the next submission collides
  # with it: `pods "spark-...-driver" already exists` (409).
  run! 'kubectl', 'delete', 'pod', '-n', ns,
       '-l', "sparkoperator.k8s.io/app-name=#{scheduled_name}-oneshot", '--ignore-not-found'
  run! 'kubectl', 'apply', '-f', file.path
  manifest['metadata']['name']
end

namespace :batch do
  desc 'Run the medallion now, in order (args: tiers=bronze,silver,gold; default: all)'
  task :run, [:tiers] do |_t, args|
    banner 'BATCH RUN'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    tiers = args[:tiers].to_s.split(',').map(&:strip).reject(&:empty?)
    tiers = %w[bronze silver gold] if tiers.empty?
    unknown = tiers - BATCH_TIERS.keys
    abort "\e[31mUnknown tier(s): #{unknown.join(', ')}. Choose from: #{BATCH_TIERS.keys.join(', ')}\e[0m" if unknown.any?

    tiers.each do |tier|
      spec = BATCH_TIERS[tier]
      info_msg("--- #{tier} ---")
      case spec[:kind]
      when 'cronjob'
        # The cron is suspended while the one-shot runs, so the 15-minute
        # schedule cannot fire in the middle of the verification.
        run! 'kubectl', 'patch', 'cronjob', spec[:object], '-n', spec[:ns],
             '-p', '{"spec":{"suspend":true}}'
        run! 'kubectl', 'delete', 'job', "#{tier}-oneshot", '-n', spec[:ns], '--ignore-not-found'
        run! 'kubectl', 'create', 'job', "--from=cronjob/#{spec[:object]}",
             "#{tier}-oneshot", '-n', spec[:ns]
        complete, = capture('kubectl', 'wait', "--for=condition=complete",
                            "job/#{tier}-oneshot", '-n', spec[:ns], '--timeout=600s')
        logs, = capture('kubectl', 'logs', "-n", spec[:ns], "job/#{tier}-oneshot", '--tail=200')
        puts logs unless logs.to_s.strip.empty?
        # The cron goes back to its schedule whatever happened.
        run! 'kubectl', 'patch', 'cronjob', spec[:object], '-n', spec[:ns],
             '-p', '{"spec":{"suspend":false}}'
        abort "\e[31m#{tier}: el Job no termino correctamente\e[0m" unless complete.to_s.include?('condition met')
        ok_msg("#{tier}: ingestion completada")
      when 'spark'
        name = submit_one_shot_spark(spec[:object], spec[:ns])
        info_msg("esperando a #{name} (timeout 15 min)...")
        finished = wait_for_spark_application(name, spec[:ns], 15 * 60)
        logs, = capture('kubectl', 'logs', '-n', spec[:ns],
                        "-l", "spark-role=driver,sparkoperator.k8s.io/app-name=#{name}",
                        '--tail=200', '--all-containers=true')
        puts logs unless logs.to_s.strip.empty?
        state, = capture('kubectl', 'get', 'sparkapplication', name, '-n', spec[:ns],
                          '-o', 'jsonpath={.status.applicationState.state}')
        unless finished
          abort "\e[31m#{tier}: la SparkApplication termino en estado #{state.to_s.strip.inspect}. " \
                "Logs arriba, o: rake cluster:diagnose[#{name}]\e[0m"
        end
        ok_msg("#{tier}: completado (estado #{state.to_s.strip})")
      end
    end

    puts
    ok_msg('Medallion ejecutada. Comprueba las filas con: rake lakehouse:rows')
  end
end

namespace :lakehouse do
  desc 'Prove the Iceberg lakehouse adapter in ONE pod: Polaris catalog -> MinIO -> Parquet'
  task :probe do
    banner 'ICEBERG LAKEHOUSE PROBE'
    script = File.join('infra', 'diagnostics', 'iceberg_adapter_probe.py')
    job_file = File.join('infra', 'diagnostics', 'iceberg-adapter-probe.job.yaml')
    abort "\e[31mMissing #{script}\e[0m" unless File.exist?(script)
    abort "\e[31mMissing #{job_file}\e[0m" unless File.exist?(job_file)

    _, api_ok = capture('kubectl', 'cluster-info')
    unless api_ok
      abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m"
    end
    up, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                  '-o', 'jsonpath={.items[*].metadata.name}')
    %w[minio polaris].each do |component|
      next if up.to_s.match?(/\A#{component}-\d/) || up.to_s.match?(/#{component}-[a-z0-9]+-/)

      abort "\e[31m#{component} is not running in namespace platform.\e[0m\n" \
            "\e[33mDeploy it first: rake cluster:up[stream]\e[0m"
    end

    run! 'kubectl', 'delete', 'job', 'iceberg-adapter-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'delete', 'configmap', 'iceberg-adapter-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'create', 'configmap', 'iceberg-adapter-probe', '-n', 'platform',
         "--from-file=iceberg_adapter_probe.py=#{script}"

    run! 'kubectl', 'apply', '-f', job_file
    info_msg('probe running; it writes a real Iceberg table, allow ~2 min...')

    deadline = Time.now + (12 * 60)
    phase = ''
    loop do
      complete, = capture('kubectl', 'get', 'job', 'iceberg-adapter-probe', '-n', 'platform',
                          '-o', 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')
      if complete.to_s.strip == 'True'
        phase = 'complete'
        break
      end
      failed, = capture('kubectl', 'get', 'job', 'iceberg-adapter-probe', '-n', 'platform',
                        '-o', 'jsonpath={.status.failed}')
      if failed.to_s.strip.to_i.positive?
        phase = 'failed'
        break
      end
      if Time.now > deadline
        phase = 'timeout'
        break
      end
      sleep 10
    end

    puts
    logs, = capture('kubectl', 'logs', '-n', 'platform', 'job/iceberg-adapter-probe')
    puts logs unless logs.to_s.strip.empty?
    puts

    # Independent of the Job's own verdict: did Parquet actually land? Asked of
    # the MinIO pod, because the probe's container has exited by now.
    minio_pod, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                         '-o', 'jsonpath={.items[?(@.metadata.labels.app=="minio")].metadata.name}')
    minio_pod = minio_pod.to_s.strip
    if minio_pod.empty?
      err_msg('no se ha encontrado el pod de MinIO para contar los parquet')
    else
      data, = capture('kubectl', 'exec', '-n', 'platform', minio_pod, '--',
                      'sh', '-c',
                      "find /data/warehouse/sales/probe_metrics/data -name '*.parquet' | wc -l")
      count = data.to_s.strip
      if count =~ /\A\d+\z/ && count.to_i.positive?
        ok_msg("MinIO contiene #{count} fichero(s) parquet en sales.probe_metrics")
      else
        err_msg("no se ha encontrado ningun parquet en el warehouse (#{data.to_s.lines.first})")
      end
    end

    case phase
    when 'complete'
      if logs.to_s.include?('RESULTADO: OK')
        ok_msg('PROBE OK: el adaptador Iceberg escribe y lee en el lakehouse real')
      else
        abort "\e[31mPROBE FALLIDO: el Job termino sin 'RESULTADO: OK'.\e[0m"
      end
    when 'failed'
      abort "\e[31mPROBE: el Job fallo (exit != 0). Lee el log de arriba.\e[0m"
    else
      abort "\e[31mPROBE: no termino en 12 min. Logs: kubectl logs -n platform job/iceberg-adapter-probe\e[0m"
    end
  end

  desc 'Count the rows the medallion left in the Iceberg lakehouse (args: tiers=bronze,silver,gold)'
  task :rows, [:tiers] do |_t, args|
    banner 'LAKEHOUSE ROWS'
    script = File.join('infra', 'diagnostics', 'lakehouse_rows_probe.py')
    job_file = File.join('infra', 'diagnostics', 'lakehouse-rows-probe.job.yaml')
    abort "\e[31mMissing #{script}\e[0m" unless File.exist?(script)
    abort "\e[31mMissing #{job_file}\e[0m" unless File.exist?(job_file)

    _, api_ok = capture('kubectl', 'cluster-info')
    unless api_ok
      abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m"
    end
    up, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                  '-o', 'jsonpath={.items[*].metadata.name}')
    %w[minio polaris].each do |component|
      next if up.to_s.match?(/\A#{component}-\d/) || up.to_s.match?(/#{component}-[a-z0-9]+-/)

      abort "\e[31m#{component} is not running in namespace platform.\e[0m\n" \
            "\e[33mDeploy it first: rake cluster:up[batch]\e[0m"
    end

    tiers = args[:tiers].to_s
    tiers = 'bronze,silver,gold' if tiers.strip.empty?

    run! 'kubectl', 'delete', 'job', 'lakehouse-rows-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'delete', 'configmap', 'lakehouse-rows-probe', '-n', 'platform',
         '--ignore-not-found'
    run! 'kubectl', 'create', 'configmap', 'lakehouse-rows-probe', '-n', 'platform',
         "--from-file=lakehouse_rows_probe.py=#{script}"

    # The tiers that have to have rows are part of the Job spec, so the
    # manifest is submitted with the value already in it (patching a Job after
    # its pod is running would change nothing).
    manifest = File.read(job_file).sub('value: bronze,silver,gold', "value: #{tiers}")
    applied, apply_ok = capture('kubectl', 'apply', '-f', '-', stdin: manifest)
    abort "\e[31mno se pudo crear el Job del probe: #{applied}\e[0m" unless apply_ok

    info_msg("contando filas (tiers: #{tiers})...")

    deadline = Time.now + (8 * 60)
    phase = ''
    loop do
      complete, = capture('kubectl', 'get', 'job', 'lakehouse-rows-probe', '-n', 'platform',
                          '-o', 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')
      if complete.to_s.strip == 'True'
        phase = 'complete'
        break
      end
      failed, = capture('kubectl', 'get', 'job', 'lakehouse-rows-probe', '-n', 'platform',
                        '-o', 'jsonpath={.status.failed}')
      if failed.to_s.strip.to_i.positive?
        phase = 'failed'
        break
      end
      if Time.now > deadline
        phase = 'timeout'
        break
      end
      sleep 10
    end

    puts
    logs, = capture('kubectl', 'logs', '-n', 'platform', 'job/lakehouse-rows-probe')
    puts logs unless logs.to_s.strip.empty?
    puts

    case phase
    when 'complete'
      if logs.to_s.include?('RESULTADO: OK')
        ok_msg('LAS FILAS ESTAN EN EL LAKEHOUSE')
      else
        abort "\e[31mPROBE FALLIDO: el Job termino sin 'RESULTADO: OK'.\e[0m"
      end
    when 'failed'
      abort "\e[31mPROBE: el Job fallo (exit != 0). Lee el log de arriba.\e[0m"
    else
      abort "\e[31mPROBE: no termino en 8 min. Logs: kubectl logs -n platform job/lakehouse-rows-probe\e[0m"
    end
  end

  desc 'Re-register the Iceberg tables in the Polaris catalogue after a Polaris restart'
  task :restore do
    banner 'LAKEHOUSE RESTORE'
    run_lakehouse_restore
  end
end

# Re-registers the Iceberg tables in the Polaris catalogue and waits for it.
#
# Polaris keeps the catalogue IN MEMORY, so every restart of the pod leaves it
# empty even though the tables and their data are still in MinIO. Anything
# that reads the lakehouse through the catalogue (Trino, the Flink speed
# layer, the row counter) then fails with:
#
#   org.apache.iceberg.exceptions.RESTException: Unable to process:
#   Unable to find warehouse polaris
#
# so this runs before every increment that reads the lakehouse.
def run_lakehouse_restore
  script = File.join('infra', 'diagnostics', 'polaris_restore.py')
  job_file = File.join('infra', 'diagnostics', 'polaris-restore.job.yaml')
  abort "\e[31mMissing #{script}\e[0m" unless File.exist?(script)
  abort "\e[31mMissing #{job_file}\e[0m" unless File.exist?(job_file)

  _, api_ok = capture('kubectl', 'cluster-info')
  unless api_ok
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m"
  end
  up, = capture('kubectl', 'get', 'pods', '-n', 'platform',
                '-o', 'jsonpath={.items[*].metadata.name}')
  %w[minio polaris].each do |component|
    next if up.to_s.match?(/\A#{component}-\d/) || up.to_s.match?(/#{component}-[a-z0-9]+-/)

    abort "\e[31m#{component} is not running in namespace platform.\e[0m\n" \
          "\e[33mDeploy it first: rake ramp:lakehouse\e[0m"
  end

  run! 'kubectl', 'delete', 'job', 'polaris-restore', '-n', 'platform',
       '--ignore-not-found'
  run! 'kubectl', 'delete', 'configmap', 'polaris-restore', '-n', 'platform',
       '--ignore-not-found'
  run! 'kubectl', 'create', 'configmap', 'polaris-restore', '-n', 'platform',
       "--from-file=polaris_restore.py=#{script}"

  applied, apply_ok = capture('kubectl', 'apply', '-f', job_file)
  abort "\e[31mno se pudo crear el Job de restore: #{applied}\e[0m" unless apply_ok

  info_msg('re-registrando las tablas en el catalogo de Polaris...')

  deadline = Time.now + (10 * 60)
  phase = ''
  loop do
    complete, = capture('kubectl', 'get', 'job', 'polaris-restore', '-n', 'platform',
                        '-o', 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')
    if complete.to_s.strip == 'True'
      phase = 'complete'
      break
    end
    failed, = capture('kubectl', 'get', 'job', 'polaris-restore', '-n', 'platform',
                      '-o', 'jsonpath={.status.failed}')
    if failed.to_s.strip.to_i.positive?
      phase = 'failed'
      break
    end
    if Time.now > deadline
      phase = 'timeout'
      break
    end
    sleep 10
  end

  puts
  logs, = capture('kubectl', 'logs', '-n', 'platform', 'job/polaris-restore')
  puts logs unless logs.to_s.strip.empty?
  puts

  case phase
  when 'complete'
    if logs.to_s.include?('RESTORE: OK')
      ok_msg('EL CATALOGO DE POLARIS TIENE LAS TABLAS')
      true
    else
      abort "\e[31mRESTORE: el Job termino sin 'RESTORE: OK'.\e[0m"
    end
  when 'failed'
    abort "\e[31mRESTORE: el Job fallo (exit != 0). Lee el log de arriba.\e[0m"
  else
    abort "\e[31mRESTORE: no termino en 10 min. Logs: kubectl logs -n platform job/polaris-restore\e[0m"
  end
end

# ------------------------------------------------------------------------------
# RF-24: the demo as a SEQUENCE of exclusive phases
# ------------------------------------------------------------------------------
#
# RC-02 gives the box 8 GB and asks for EXCLUSIVE startup profiles; AD-4 says
# "Demo por perfiles". The demo profile (all eight subsystems at once) needs
# ~8000 MiB of container limits and does not fit on a 6 GiB host: the
# kube-scheduler ended up losing its lease 107 times. So the demo is a
# sequence, and each phase declares what it brings up AND what the next phase
# cannot afford.
#
# What crosses the phases is the LAKEHOUSE (MinIO + Iceberg), not a pod: that
# is why the batch tier and the stream tier can run in different phases and
# still be queried together at the end (RF-11).
#
#   phase      up                                  down (for the next phase)
#   platform   postgres, minio, polaris, marquez   -
#   batch      + spark operator, medallion runs    -
#   stream     kafka, flink operator, flink job    postgres, spark operator,
#                                                   marquez, lineage webhook
#   serving    trino                                flink job, flink operator
#   fabric     marquez + lineage webhook back up    trino
#   ops        prometheus, grafana (+ kafka up)     flink operator
#   governance kyverno controller + policies        everything else
# ------------------------------------------------------------------------------
DEMO_PHASES = [
  {
    'name' => 'platform',
    'profile' => 'platform',
    'charts' => %w[s1-ingestion s2-lakehouse s7-fabric],
    'wait' => [['platform', 'deploy/postgres-oltp'], ['platform', 'deploy/minio'],
               ['platform', 'deploy/polaris'], ['platform', 'deploy/marquez'],
               ['platform', 'deploy/lineage-alert-service']]
  },
  {
    'name' => 'batch',
    'profile' => 'batch',
    'charts' => %w[s1-ingestion s2-lakehouse s3-batch],
    'wait' => [['platform', 'deploy/spark-operator']]
  },
  {
    'name' => 'stream',
    'profile' => 'stream',
    # s2 is NOT upgraded here: the lakehouse release is already installed and
    # serving, and its bootstrap hook needs 6-11 minutes on this box (the
    # whole cluster is swapping while it runs). Only s4 comes up. The speed
    # layer writes into the lakehouse that is already there.
    'charts' => %w[s4-stream],
    'down' => [['scale', 'platform', 'deploy/postgres-oltp', 0],
               ['scale', 'platform', 'deploy/spark-operator', 0],
               ['scale', 'platform', 'deploy/marquez', 0],
               ['scale', 'platform', 'deploy/lineage-alert-service', 0],
               ['rmimage', 'docker.io/poc/spark-medallion:0.1.0'],
               # The hook Jobs of an upgrade that timed out stay behind, and
               # Helm then refuses to touch them ("invalid ownership
               # metadata"): they belong to no release.
               ['delete', 'job', 'polaris-catalog-bootstrap', 'platform'],
               ['delete', 'job', 'polaris-catalog-registration', 'platform'],
               ['delete', 'job', 'minio-bucket-init', 'platform'],
               ['delete', 'job', 'kafka-topic-creator', 'platform']],
    'wait' => [['platform', 'statefulset/kafka']]
  },
  {
    'name' => 'serving',
    'profile' => 'serving',
    'charts' => %w[s5-serving],
    'down' => [['delete', 'flinkdeployment', 'flink-orders-stream', 'sales'],
               ['scale', 'platform', 'deploy/flink-kubernetes-operator', 0],
               # Kafka is a StatefulSet, not a Deployment: scaling
               # "deploy/kafka" fails with "the server doesn't have a resource
               # type deploy" and the broker stays up with its 768Mi of limits,
               # which is what the serving phase cannot afford.
               ['scale', 'platform', 'statefulset/kafka', 0]],
    'wait' => [['platform', 'deploy/trino']]
  },
  {
    'name' => 'fabric',
    'profile' => 'platform',
    'charts' => %w[s7-fabric],
    # Kafka is not part of the lineage: it comes down so the fabric phase has
    # room for Marquez, its database and the alert service.
    'down' => [['scale', 'platform', 'deploy/trino', 0],
               ['scale', 'platform', 'statefulset/kafka', 0],
               ['scale', 'platform', 'deploy/postgres-oltp', 0]],
    'wait' => [['platform', 'deploy/marquez'], ['platform', 'deploy/lineage-alert-service']]
  },
  {
    'name' => 'ops',
    'profile' => 'demo',
    'charts' => %w[s8-ops],
    # Kafka comes BACK up: Prometheus scrapes it (kafka_consumergroup_lag), and
    # a scrape target that is down is not a verification.
    'down' => [['scale', 'platform', 'deploy/flink-kubernetes-operator', 0],
               ['scale', 'platform', 'statefulset/kafka', 1]],
    'wait' => [['platform', 'deploy/s8-ops-prometheus'], ['platform', 'deploy/s8-ops-grafana']]
  },
  {
    'name' => 'governance',
    'profile' => 'demo',
    'charts' => %w[s6-mesh],
    'down' => [['scale', 'platform', 'statefulset/kafka', 0],
               ['scale', 'platform', 'deploy/s8-ops-prometheus', 0],
               ['scale', 'platform', 'deploy/s8-ops-grafana', 0],
               ['scale', 'platform', 'deploy/marquez', 0],
               ['scale', 'platform', 'deploy/lineage-alert-service', 0]],
    # The s6-mesh chart only APPLIES the ClusterPolicies; the controller and
    # its CRDs come from the upstream chart, which the cluster:create task
    # already registers (https://kyverno.github.io/kyverno/).
    'extra' => [%w[helm upgrade --install kyverno kyverno/kyverno -n kyverno --create-namespace]]
  }
].freeze


# Installs the given charts of a profile. Returns the number of failures.
#
# `sets` maps a chart name to its --set overrides:
#   helm_upgrade_charts('stream', %w[s2-lakehouse s4-stream],
#                       's4-stream' => { 'flinkJobs.ordersStream.enabled' => 'false' })
def helm_upgrade_charts(profile, charts, sets = {})
  failures = 0
  charts.each do |chart|
    unless File.directory?(File.join('infra', 'helm', chart))
      err_msg("MISSING: infra/helm/#{chart} not found")
      failures += 1
      next
    end
    values = extract_profile_values(profile, chart)
    if values == :error
      err_msg("could not extract 'subsystems.#{chart}' from #{profile}.yaml")
      failures += 1
      next
    end
    if values.nil?
      warn_msg("no '#{chart}' section in #{profile}; installing with chart defaults")
    end
    info_msg("deploying Helm chart: #{chart}")
    if helm_upgrade(chart, values&.path, sets[chart] || {})
      ok_msg(chart)
    else
      err_msg(chart)
      failures += 1
    end
    values&.unlink
  end
  failures
end

# Applies one "down" entry of a phase: scale a workload to zero replicas, or
# delete a custom resource. Scaling to zero keeps the PVCs, which is what
# the lakehouse and the lineage database need.
def apply_down(entry)
  case entry[0]
  when 'scale'
    _kind, ns, target, replicas = entry
    _, ok = capture('kubectl', 'scale', target, '-n', ns, "--replicas=#{replicas}")
    if ok
      info_msg("down: #{target} -> #{replicas} replicas")
    else
      warn_msg("down: could not scale #{target} (it may not be installed)")
    end
  when 'delete'
    _kind, kind, name, ns = entry
    _, ok = capture('kubectl', 'delete', kind, name, '-n', ns, '--ignore-not-found')
    ok ? info_msg("down: #{kind}/#{name} deleted") : warn_msg("down: could not delete #{kind}/#{name}")
  when 'rmimage'
    # An image that this phase does not use costs a gigabyte of page cache
    # per node on a 6 GiB box, and that cache is what makes the API server
    # time out. The Spark image is only needed by the batch phase.
    _kind, image = entry
    probe = podman_probe
    probe[:containers].each do |node|
      [image, image.sub('docker.io/', 'localhost/')].each do |ref|
        capture('podman', '--connection', node[:connection], 'exec', node[:name],
                'ctr', '-n', 'k8s.io', 'images', 'rm', ref)
      end
    end
    info_msg("down: removed #{image} from every node")
  else
    warn_msg("down: unknown action #{entry[0]}")
  end
end

# Waits for a workload to be ready. `target` is "deploy/<name>" or
# "statefulset/<name>".
def wait_for_workload(ns, target, timeout_s = 300)
  out, ok = capture('kubectl', 'rollout', 'status', target, '-n', ns, "--timeout=#{timeout_s}s")
  if ok
    ok_msg("#{ns}/#{target} ready")
  else
    warn_msg("#{ns}/#{target} not ready: #{out.to_s.lines.last.to_s.strip}")
  end
  ok
end

# ------------------------------------------------------------------------------
# RAMP: the platform as a ladder of MINIMAL increments
# ------------------------------------------------------------------------------
#
# The podman machine that backs kind has 5.9 GiB of RAM, and the three kind
# node CONTAINERS are not free: measured from the host they are 2.0 + 1.5 +
# 1.6 GB. The cluster itself costs ~3.5 GB before a single application pod
# starts. Worse, every node REPORTS ~5.79 GiB of allocatable memory (they all
# see the same VM), so the scheduler happily commits 17.4 GiB on a 5.9 GiB
# machine and the kubelet then evicts whatever is unlucky - which is what made
# the failures look random (minio's liveness probe timing out, marquez
# OOM-killed, the scheduler losing its lease 107 times).
#
# So the platform is deployed in INCREMENTS. Each one brings up ONLY the pods
# its test needs and takes the rest down, and the ledger (rake ramp:status)
# prints the sum of the limits of the running pods: that sum, not the
# scheduler's opinion, is the number that has to stay under the box.
#
#   ramp:lakehouse  minio + polaris                    -> the catalogue
#   ramp:kafka      + kafka                            -> the topics
#   ramp:stream     + flink operator + flink job       -> the speed layer
#   ramp:serving    + trino (kafka and flink down)     -> the unified view
#
# The lakehouse crosses the increments: the data lives in MinIO, not in a pod,
# which is why the speed layer and the serving layer see the same gold rows.
#
# s2-lakehouse is upgraded ONCE, by ramp:lakehouse, and never again. Its
# chart has three Jobs that run on EVERY upgrade - the Polaris bootstrap (it
# re-creates the catalogue, which WIPES it), the catalogue registration and
# the bucket initialisation - and that burst of pods is what starves MinIO
# and the API server on this box:
#
#   FALLO: sales.bronze_orders: error al leer (AWS Error NETWORK_CONNECTION
#   during GetObject operation: curlCode: 28, Timeout was reached)
#
# The limits it needs are the same in the stream and the serving profile, so
# there is nothing to change by re-upgrading it.
#
# The heavy increments additionally STOP the "worker" kind node, which is
# where 1.5 GB of the overhead sits. The node comes back with
# `rake ramp:node:start`.
# ------------------------------------------------------------------------------

# The kind node the heavy phases stop. It carries the "storage" role
# (MinIO's PVC is on worker2, but Kafka's, Postgres' and Marquez' are on this
# one), so stopping it is only possible when the workloads that need a
# node-local volume are not in the increment.
RAMP_HEAVY_NODE = 'poc-data-platform-worker'.freeze

# The node the heavy workloads run on: the Flink operator's image and the
# lakehouse's live there, so the 1.7 GB Flink image only has to be loaded
# once.
RAMP_COMPUTE_NODE = 'poc-data-platform-worker2'.freeze

# Everything that is never part of an increment. Scaled to zero, not
# uninstalled: the PVCs of the lineage database and the lakehouse are what
# carry the data across the phases.
RAMP_COMMON_DOWN = [
  ['scale', 'platform', 'deploy/postgres-oltp', 0],
  ['scale', 'platform', 'deploy/spark-operator', 0],
  ['scale', 'platform', 'deploy/marquez', 0],
  ['scale', 'platform', 'deploy/marquez-postgresql', 0],
  ['scale', 'platform', 'deploy/lineage-alert-service', 0],
  ['scale', 'platform', 'deploy/s8-ops-prometheus', 0],
  ['scale', 'platform', 'deploy/s8-ops-grafana', 0],
  ['delete', 'flinkdeployment', 'flink-orders-stream', 'sales']
].freeze

# Converts a human memory size ("1.615GB", "384Mi", "512MiB") to MiB.
#
# The unit letter is required and the trailing "B" is optional: Kubernetes
# writes "320Mi" and podman writes "1.615GB", so a pattern that expects the
# trailing B misses every Kubernetes quantity.
#
# The first match is taken instead of the whole string: the jsonpath below
# concatenates the limits of a multi-container pod into one field.
def mem_to_mib(text)
  m = text.to_s.strip.match(/([\d.]+)\s*([KMG])i?B?/i)
  return 0 if m.nil?

  factor = case m[2].upcase
           when 'K' then 1.0 / 1024
           when 'M' then 1.0
           when 'G' then 1024.0
           else 0.0
           end
  (m[1].to_f * factor).round
end

# The RAM ledger: the running pods with their memory limits, their sum, and
# the memory the kind node containers are holding. The sum is what the box
# actually has to give, because the scheduler believes there is 17.4 GiB.
def ram_ledger
  # `containers[*]` and not `containers[0]`: inside a `range`, kubectl's
  # jsonpath does NOT resolve the indexed form and returns an empty string
  # for the limit, which made the whole ledger read 0 MiB.
  out, ok = capture('kubectl', 'get', 'pods', '-A',
                    '-o', 'jsonpath={range .items[*]}{.metadata.namespace}{" "}' \
                    '{.metadata.name}{" "}{.status.phase}{" "}' \
                    '{.spec.containers[*].resources.limits.memory}{"\n"}{end}')
  pods = []
  if ok
    out.lines.each do |line|
      ns, name, phase, limit = line.strip.split(/\s+/, 4)
      next if ns.nil? || phase != 'Running'

      pods << { ns: ns, name: name, mib: mem_to_mib(limit) }
    end
  end

  probe = podman_probe
  overhead = 0
  probe[:containers].each do |container|
    stats, stats_ok = capture('podman', '--connection', container[:connection], 'stats',
                               '--no-stream', '--format', '{{.MemUsage}}', container[:name])
    next unless stats_ok

    overhead += mem_to_mib(stats.to_s.split('/').first.to_s.strip)
  end

  # The kube-system pods are NOT part of the budget: they are the control
  # plane, they run whether the platform is up or not, and counting them
  # twice (once here, once inside the kind node containers) makes the number
  # meaningless.
  apps = pods.reject { |p| p[:ns] == 'kube-system' || p[:ns] == 'local-path-storage' }
  {
    pods: pods,
    apps_mib: apps.sum { |p| p[:mib] },
    limits_mib: pods.sum { |p| p[:mib] },
    kind_mib: overhead
  }
end

# Prints the ledger and says whether the box has room for what comes next.
def ramp_report(budget_mib = 1600)
  ledger = ram_ledger
  banner 'RAM LEDGER'
  ledger[:pods].sort_by { |p| -p[:mib] }.each do |pod|
    marker = pod[:ns] == 'kube-system' || pod[:ns] == 'local-path-storage' ? ' ' : '*'
    puts format('%s %-10s %-44s %6s MiB', marker, pod[:ns], pod[:name], pod[:mib])
  end
  puts
  puts format('  %-55s %6d MiB', 'sum of the APPLICATION pods LIMITS', ledger[:apps_mib])
  puts format('  %-55s %6d MiB', 'sum of ALL the running pods limits', ledger[:limits_mib])
  puts format('  %-55s %6d MiB', 'kind node containers (measured from the host)', ledger[:kind_mib])
  puts
  if ledger[:apps_mib] > budget_mib
    warn_msg("the application pods ask for #{ledger[:apps_mib]} MiB, over the #{budget_mib} MiB budget")
  else
    ok_msg("the application pods ask for #{ledger[:apps_mib]} MiB of the #{budget_mib} MiB budget")
  end
  ledger
end

# Stops a kind node: drain it first so the API server evicts its pods, then
# stop the container so its kubelet stops reporting.
def ramp_node_stop(name)
  probe = podman_probe
  container = probe[:containers].find { |c| c[:name] == name }
  if container.nil?
    warn_msg("kind node container #{name} not found")
    return false
  end

  unless container_running?(container)
    ok_msg("kind node #{name} is already stopped")
    return true
  end

  info_msg("draining #{name}...")
  _out, ok = capture('kubectl', 'drain', name, '--ignore-daemonsets', '--delete-emptydir-data',
                     '--force', '--timeout=180s')
  warn_msg("the drain of #{name} did not finish cleanly") unless ok

  # The hook Jobs are deleted BEFORE the node goes away. A pod of a node that
  # is not there cannot be deleted (there is no kubelet to confirm the kill),
  # so it stays Terminating forever, and a hook Job whose pod is stuck
  # Terminating blocks the next hook of the SAME NAME:
  #   pre-upgrade hooks failed: resource Job/platform/kafka-topic-creator
  #   not ready. status: Failed, message: Job Failed. failed: 1/1
  capture('kubectl', 'delete', 'job', 'kafka-topic-creator', '-n', 'platform',
          '--ignore-not-found', '--force', '--grace-period=0')

  info_msg("stopping the #{name} container...")
  _out, ok = capture('podman', '--connection', container[:connection], 'stop', name)
  unless ok
    err_msg("could not stop #{name}")
    return false
  end
  ok_msg("#{name} stopped (#{name} was holding ~1.5 GB of the VM)")
  true
end

# Starts a kind node back and waits for it to be Ready again.
def ramp_node_start(name)
  probe = podman_probe
  container = probe[:containers].find { |c| c[:name] == name }
  if container.nil?
    err_msg("kind node container #{name} not found")
    return false
  end

  unless container_running?(container)
    info_msg("starting the #{name} container...")
    _out, ok = capture('podman', '--connection', container[:connection], 'start', name)
    unless ok
      err_msg("could not start #{name}")
      return false
    end
  end

  capture('kubectl', 'uncordon', name)
  info_msg("waiting for #{name} to be Ready...")
  deadline = Time.now + (5 * 60)
  loop do
    ready, = capture('kubectl', 'get', 'node', name,
                     '-o', 'jsonpath={.status.conditions[?(@.type=="Ready")].status}')
    break if ready.to_s.strip == 'True'
    if Time.now > deadline
      err_msg("#{name} did not become Ready in 5 min")
      return false
    end
    sleep 5
  end
  ok_msg("#{name} is Ready again")
  true
end

# Deletes the Kafka StatefulSet and its volume.
#
# Needed because `persistence.enabled` is part of the StatefulSet's
# volumeClaimTemplates, and those are IMMUTABLE: switching the flag from true
# to false on a live StatefulSet fails with "updates to statefulset spec for
# fields other than 'replicas' are forbidden". The broker's log is a
# throwaway buffer - the events are re-produced on every run and the topics
# are recreated by the hook - so dropping it costs nothing.
def ramp_kafka_reset
  capture('kubectl', 'delete', 'statefulset/kafka', '-n', 'platform', '--ignore-not-found')
  capture('kubectl', 'delete', 'pvc', 'data-kafka-0', '-n', 'platform', '--ignore-not-found')
  capture('kubectl', 'delete', 'job', 'kafka-topic-creator', '-n', 'platform', '--ignore-not-found')

  # The ControllerRevisions of the old StatefulSet have to go with it. They
  # are named after a HASH of the pod template, so the next StatefulSet
  # adopts the leftovers, and then currentRevision and updateRevision point
  # at a revision that is NOT the pod's: the rolling update never fires and
  # the broker keeps running the old template (and the old heap) forever.
  #
  #   currentRev=kafka-8667856cd4 updateRev=kafka-8667856cd4
  #   podHash=kafka-5c76cdddfc
  capture('kubectl', 'delete', 'controllerrevision', '-n', 'platform',
          '--selector', 'app=kafka', '--ignore-not-found')
end

# Loads an image into ONE kind node.
#
# cluster:image:load copies into EVERY node, and on this box that is several
# GB of writes: the Flink image alone is 1.7 GB, and the filesystem (a WSL2
# rootfs on the Windows one) does not take that without starving etcd and
# killing the API server. The ramp increments pin the workloads to one node,
# so one copy is enough.
def ramp_load_image(image, node_name)
  probe = podman_probe
  node = probe[:containers].find { |c| c[:name] == node_name }
  if node.nil?
    err_msg("kind node #{node_name} not found")
    return false
  end
  load_image_into(node, image)
end

# The topic creator is a PRE-upgrade hook: it runs BEFORE Helm applies the
# release, so the broker it has to talk to has to be up already. When the
# previous increment scaled Kafka down, the hook waits 300s for a broker that
# does not exist yet and the whole upgrade fails with
#   pre-upgrade hooks failed: resource Job/platform/kafka-topic-creator not
#   ready. status: InProgress, message: Job in progress
def ramp_broker_up
  capture('kubectl', 'scale', 'statefulset/kafka', '-n', 'platform', '--replicas=1')
  wait_for_workload('platform', 'statefulset/kafka', 300)
end

namespace :ramp do
  desc 'Show the RAM ledger: the running pods, their limits and the kind overhead'
  task :status do
    banner 'RAMP STATUS'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    ramp_report
    puts
    info_msg('next increments: ramp:lakehouse -> ramp:kafka -> ramp:stream -> ramp:serving')
  end

  desc 'Increment I0: the lakehouse only (MinIO + Polaris) and its catalogue'
  task :lakehouse do
    banner 'RAMP I0 - LAKEHOUSE'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    RAMP_COMMON_DOWN.each { |entry| apply_down(entry) }
    [['scale', 'platform', 'statefulset/kafka', 0],
     ['scale', 'platform', 'deploy/flink-kubernetes-operator', 0],
     ['scale', 'platform', 'deploy/trino', 0]].each { |entry| apply_down(entry) }

    failures = helm_upgrade_charts('stream', %w[s2-lakehouse])
    abort "\e[31ms2-lakehouse did not install cleanly.\e[0m" unless failures.zero?

    wait_for_workload('platform', 'deploy/minio')
    wait_for_workload('platform', 'deploy/polaris')
    ramp_report
    info_msg('next: rake lakehouse:restore (the catalogue is empty after a Polaris restart)')
  end

  desc 'Increment I1: the lakehouse plus the Kafka broker and its topics'
  task :kafka do
    banner 'RAMP I1 - LAKEHOUSE + KAFKA'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    RAMP_COMMON_DOWN.each { |entry| apply_down(entry) }
    [['scale', 'platform', 'deploy/flink-kubernetes-operator', 0],
     ['scale', 'platform', 'deploy/trino', 0]].each { |entry| apply_down(entry) }

    # The Flink job stays off in this increment: the broker and the topics are
    # what this one proves, and the speed layer is the increment that does not
    # fit without freeing a node.
    #
    # Kafka runs WITHOUT a volume from here on: the log is disposable, and
    # without a PVC the broker can be scheduled on any node, which is what
    # lets the heavy increment stop one.
    ramp_kafka_reset

    failures = helm_upgrade_charts('stream', %w[s4-stream],
                                    's4-stream' => { 'flinkJobs.ordersStream.enabled' => 'false',
                                                    'flinkOperator.enabled' => 'false',
                                                    'kafka.persistence.enabled' => 'false' })
    abort "\e[31mthe charts did not install cleanly.\e[0m" unless failures.zero?

    wait_for_workload('platform', 'deploy/minio')
    wait_for_workload('platform', 'deploy/polaris')
    wait_for_workload('platform', 'statefulset/kafka')
    ramp_report
  end

  desc 'Increment I2: the speed layer (Kafka + Flink). Stops the worker node'
  task :stream do
    banner 'RAMP I2 - SPEED LAYER'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    RAMP_COMMON_DOWN.each { |entry| apply_down(entry) }
    [['scale', 'platform', 'deploy/trino', 0]].each { |entry| apply_down(entry) }

    # The Flink image has to be in the node the job runs on: it is 1.7 GB
    # and loading it into every node is several GB of writes, which on this
    # filesystem (a WSL2 rootfs on the Windows one) starves etcd and kills
    # the API server. The job is pinned to the compute node.
    unless ramp_load_image('docker.io/poc/flink-stream:0.1.0', RAMP_COMPUTE_NODE)
      abort "\e[31mthe Flink image could not be loaded into #{RAMP_COMPUTE_NODE}.\e[0m"
    end

    # The node is stopped BEFORE the upgrade, not after it.
    #
    # Starting the Flink JobManager and TaskManager on top of the cluster as
    # it is pushes the box past its memory, and the API server dies first:
    # the RBAC error Helm reports ("kubernetes-admin cannot list jobs",
    # "cluster-admin not found") is a SYMPTOM of the apiserver restarting,
    # not a permission problem. Stopping the node first frees 1.5 GB, and
    # the apiserver survives the arrival of the speed layer.
    ramp_node_stop(RAMP_HEAVY_NODE)

    # Kafka's PVC lived on the node this increment stops, and a Pod cannot be
    # scheduled on a node that is not there: the broker's log is a throwaway
    # buffer (the events are re-produced on every run), so it runs without a
    # volume in this increment.
    # The catalogue is restored BEFORE the speed layer starts. Polaris keeps
    # it in memory, so it is empty after every restart of the pod, and the
    # Flink job cannot even start without it:
    #   org.apache.iceberg.exceptions.RESTException: Unable to process:
    #   Unable to find warehouse polaris
    run_lakehouse_restore

    ramp_broker_up
    ramp_kafka_reset

    failures = helm_upgrade_charts('stream', %w[s4-stream],
                                    's4-stream' => { 'kafka.persistence.enabled' => 'false' })
    abort "\e[31mthe charts did not install cleanly.\e[0m" unless failures.zero?

    wait_for_workload('platform', 'deploy/minio')
    wait_for_workload('platform', 'deploy/polaris')
    wait_for_workload('platform', 'statefulset/kafka')
    wait_for_workload('platform', 'deploy/flink-kubernetes-operator')

    ramp_report
    info_msg('next: rake stream:produce:cluster, then check the Flink job')
  end

  desc 'Increment I3: the serving layer (Trino). Kafka and Flink go down'
  task :serving do
    banner 'RAMP I3 - SERVING LAYER'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    RAMP_COMMON_DOWN.each { |entry| apply_down(entry) }
    [['delete', 'flinkdeployment', 'flink-orders-stream', 'sales'],
     ['scale', 'platform', 'deploy/flink-kubernetes-operator', 0],
     ['scale', 'platform', 'statefulset/kafka', 0]].each { |entry| apply_down(entry) }

    failures = helm_upgrade_charts('serving', %w[s5-serving])
    abort "\e[31mthe charts did not install cleanly.\e[0m" unless failures.zero?

    wait_for_workload('platform', 'deploy/minio')
    wait_for_workload('platform', 'deploy/polaris')
    wait_for_workload('platform', 'deploy/trino')
    ramp_report
    info_msg('next: query iceberg.serving.sales_unified_revenue')
  end

  namespace :node do
    desc 'Stop the worker kind node (frees ~1.5 GB)'
    task :stop do
      banner 'RAMP NODE STOP'
      ramp_node_stop(RAMP_HEAVY_NODE)
      ramp_report
    end

    desc 'Start the worker kind node back'
    task :start do
      banner 'RAMP NODE START'
      ramp_node_start(RAMP_HEAVY_NODE)
      ramp_report
    end
  end

  desc 'Load an image into ONE kind node (args: image=... node=...; default: the compute node)'
  task :load, [:image, :node] do |_t, args|
    banner 'RAMP IMAGE LOAD'
    image = args[:image].to_s.strip
    node = args[:node].to_s.strip
    node = RAMP_COMPUTE_NODE if node.empty?
    abort "\e[31mMissing the image to load. Usage: rake ramp:load[docker.io/poc/flink-stream:0.1.0]\e[0m" if image.empty?

    ok = ramp_load_image(image, node)
    abort "\e[31m#{image} could not be loaded into #{node}.\e[0m" unless ok
    ok_msg("#{image} is in #{node}")
  end
end

namespace :serving do
  # The Trino CLI CANNOT be run with "kubectl exec" inside the Trino pod: the
  # server already runs its own JVM with a 640M heap under a 1024Mi limit, and
  # the second JVM dies before it prints anything:
  #   Failed to start thread "Unknown thread" - pthread_create failed (EAGAIN)
  # It is run as a throwaway pod instead, with the same image (already in the
  # node) and the server reached through its Service.
  desc 'Run a SQL query with the Trino CLI in its own pod (args: sql="...")'
  task :query, [:sql] do |_t, args|
    sql = args[:sql].to_s.strip
    abort "\e[31mMissing the query. Usage: rake serving:query[\"SELECT 1\"]\e[0m" if sql.empty?

    run! 'kubectl', 'run', 'trino-query', '--rm', '-i', '--restart=Never',
         '--image=' + trino_image, '--image-pull-policy=IfNotPresent',
         '--command', '--', 'trino',
         '--server', 'http://trino.platform.svc.cluster.local:8080',
         '--output-format', 'ALIGNED', '--execute', sql
  end

  desc 'RF-11 acceptance: the view carries both tiers and has no duplicated keys'
  task :verify do
    banner 'RF-11 UNIFIED VIEW'
    Rake::Task['serving:query'].reenable
    Rake::Task['serving:query'].invoke(
      'SELECT source_tier, count(*) AS filas FROM iceberg.serving.sales_unified_revenue GROUP BY 1 ORDER BY 1'
    )
    Rake::Task['serving:query'].reenable
    Rake::Task['serving:query'].invoke(
      'SELECT count(*) AS claves_duplicadas FROM (SELECT order_date, status ' \
      'FROM iceberg.serving.sales_unified_revenue GROUP BY 1,2 HAVING count(*) > 1)'
    )
  end
end

namespace :demo do
  desc 'RF-24 end-to-end demo as a sequence of exclusive phases (args: phases=platform,batch,...; default: all)'
  task :phases, [:phases] do |_t, args|
    banner 'DEMO PHASES'
    _, api_ok = capture('kubectl', 'cluster-info')
    abort "\e[31mNo reachable cluster. Start it first: rake cluster:start\e[0m" unless api_ok

    wanted = args[:phases].to_s.split(',').map(&:strip).reject(&:empty?)
    wanted = DEMO_PHASES.map { |p| p['name'] } if wanted.empty?
    unknown = wanted - DEMO_PHASES.map { |p| p['name'] }
    abort "\e[31mUnknown phase(s): #{unknown.join(', ')}\e[0m" if unknown.any?

    DEMO_PHASES.each do |phase|
      next unless wanted.include?(phase['name'])

      puts
      banner "PHASE #{phase['name'].upcase}"
      charts = phase['charts'] || PROFILE_CHARTS.fetch(phase['profile'])

      # What the previous phase leaves behind goes DOWN FIRST: on a 6 GiB box
      # the room for the next phase has to be there before its hooks run,
      # and the Helm hooks need 6-11 minutes each while the cluster is
      # swapping.
      (phase['down'] || []).each { |entry| apply_down(entry) }

      failures = helm_upgrade_charts(phase['profile'], charts)
      abort "\e[31mPhase #{phase['name']}: #{failures} chart(s) failed to deploy.\e[0m" if failures.positive?

      (phase['extra'] || []).each do |command|
        info_msg("running: #{command.join(' ')}")
        out, ok = capture(*command)
        puts out unless out.to_s.strip.empty?
        abort "\e[31mPhase #{phase['name']}: '#{command.join(' ')}' failed\e[0m" unless ok
      end

      (phase['wait'] || []).each { |ns, target| wait_for_workload(ns, target) }
    end

    puts
    ok_msg('All the requested phases are up. Next: the checks of each phase')
  end
end

desc 'RF-24 end-to-end demo on the cluster: code verification -> phases -> medallion -> events -> Trino'
task :demo do
  puts '==> Verifying code (the only local step)'
  %w[governance:validate lint typecheck test].each { |t| Rake::Task[t].invoke }

  puts '==> Deploying the phases (platform -> batch -> stream -> serving)'
  Rake::Task['demo:phases'].invoke('platform,batch,stream,serving')

  puts '==> Running the medallion now (bronze -> silver -> gold)'
  Rake::Task['batch:run'].invoke

  puts '==> Counting the rows the medallion left in the lakehouse'
  Rake::Task['lakehouse:rows'].invoke

  puts '==> Publishing events into Kafka for 60 s'
  Rake::Task['stream:produce'].invoke

  puts '==> Waiting one window plus watermark before reading back'
  sleep 15

  puts '==> Querying the speed layer through Trino (batch + stream, RF-11)'
  run! 'kubectl', 'run', 'trino-cli', '--rm', '-i', '--restart=Never', '--image=' + trino_image,
       '--command', '--', 'trino', '--server', 'http://trino.platform.svc.cluster.local:8080',
       '--execute',
       'SELECT status, count(*) AS windows, sum(window_orders) AS orders, sum(window_revenue) AS revenue FROM iceberg.sales.orders_windowed GROUP BY status ORDER BY status'

  puts '==> Querying the unified serving view (RF-11)'
  run! 'kubectl', 'run', 'trino-unified', '--rm', '-i', '--restart=Never', '--image=' + trino_image,
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
