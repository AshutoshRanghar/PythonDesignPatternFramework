from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    """ABSTRACT CLASS (Abstraction + Inheritance)"""
    @abstractmethod
    def extract(self):
        pass


    @abstractmethod
    def transform(self, df):
        pass


    @abstractmethod
    def load(self, df):
        pass


    def run(self):
        """Template Method Pattern"""
        df = self.extract()
        df = self.transform(df)
        self.load(df)
        return "SUCCESS"