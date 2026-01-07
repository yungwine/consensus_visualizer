from abc import ABC, abstractmethod
from src.models import ConsensusData


class Parser(ABC):
    @abstractmethod
    def parse(self) -> ConsensusData:
        pass
