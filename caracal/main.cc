#include <unistd.h>
#include <cstdio>
#include <string_view>

#include "module.h"
#include "node_config.h"
#include "tcp_node.h"
#include "console.h"
#include "log.h"
#include "epoch.h"
#include "opts.h"

void show_usage(const char *progname)
{
  printf("Usage: %s -w workload -n node_name -c controller_ip -p cpu_count -s cpu_core_shift -m\n\n", progname);
  puts("\t-w\tworkload name");
  puts("\t-n\tnode name");
  puts("\t-c\tcontroller IP address");

  puts("\nSee opts.h for extented options.");

  std::exit(-1);
}

namespace felis {

YcsbType ycsb_type = YcsbType::YCSB_A;
bool full_read = true;

void ParseControllerAddress(std::string arg);

}

using namespace felis;

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;
  std::string node_name;

  while ((opt = getopt(argc, argv, "w:n:c:X:y:r:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 'n':
        node_name = std::string(optarg);
        break;
      case 'y':
        if (std::string(optarg) == "ycsba")
          ycsb_type = YcsbType::YCSB_A;
        else if (std::string(optarg) == "ycsbb")
          ycsb_type = YcsbType::YCSB_B;
        else if (std::string(optarg) == "ycsbc")
          ycsb_type = YcsbType::YCSB_C;
        else if (std::string(optarg) == "ycsbf")
          ycsb_type = YcsbType::YCSB_F;
        else {
          logger->error("Unknown YCSB type: {}", optarg);
          std::abort();
        }
        break;
      case 'r':
        if (std::string(optarg) == "full")
          full_read = true;
        else if (std::string(optarg) == "field")
          full_read = false;
        else {
          logger->error("Unknown read type: {}", optarg);
          std::abort();
        }
        break;
      case 'c':
        ParseControllerAddress(std::string(optarg));
        break;
      case 'X':
        if (!Options::ParseExtentedOptions(std::string(optarg))) {
          fprintf(stderr, "Ignoring extended argument %s\n", optarg);
          std::exit(-1);
        }
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }


  NodeConfiguration::g_nr_threads = Options::kCpu.ToInt("4");
  NodeConfiguration::g_data_migration = Options::kDataMigration;
  if (Options::kEpochSize)
    EpochClient::g_txn_per_epoch = Options::kEpochSize.ToInt();

  Module<CoreModule>::ShowAllModules();
  Module<WorkloadModule>::ShowAllModules();
  puts("\n");

  if (node_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  auto &console = util::Instance<Console>();
  console.set_server_node_name(node_name);

  Module<CoreModule>::InitRequiredModules();

  util::InstanceInit<NodeConfiguration>();
  util::Instance<NodeConfiguration>().SetupNodeName(node_name);
  util::InstanceInit<TcpNodeTransport>();

  // init tables from the workload module
  Module<WorkloadModule>::InitModule(workload_name);

  auto client = EpochClient::g_workload_client;
  logger->info("Generating Benchmarks...");
  client->GenerateBenchmarks();

  console.UpdateServerStatus(Console::ServerStatus::Listening);
  logger->info("Ready. Waiting for run command from the controller.");
  console.WaitForServerStatus(felis::Console::ServerStatus::Running);

  abort_if(EpochClient::g_workload_client == nullptr,
           "Workload Module did not setup the EpochClient properly");

  printf("\n");
  logger->info("Starting workload");
  client->Start();

  console.WaitForServerStatus(Console::ServerStatus::Exiting);
  go::WaitThreadPool();

  return 0;
}
