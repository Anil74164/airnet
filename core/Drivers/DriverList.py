import DjangoSetup
from core.Drivers.AirVeda import AirVeda
from core.Drivers.Respirer import respirer
from core.Drivers.sensit_ramp import sensit_ramp

driverList = {
    "airveda" : AirVeda,
    "respirer": respirer,
    "sensit_ramp": sensit_ramp
}


