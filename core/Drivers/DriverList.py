from core.Drivers.AirVeda import AirVeda


class driverList:
    @staticmethod
    def driver():
        drivers = {}
        drivers['airveda'] = AirVeda
        return drivers

