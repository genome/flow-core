from flow.orchestrator.service_interface import OrchestratorServiceInterface

import flow.interfaces
import injector


class OrchestratorConfiguration(injector.Module):
    @injector.provides(flow.interfaces.IOrchestrator)
    def provide_broker(self):
        return self.__injector__.get(OrchestratorServiceInterface)
