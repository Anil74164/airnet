import DjangoSetup
from core.Drivers.AirVeda import AirVeda
from core.Drivers.Respirer import respirer
from core.Drivers.sensit_ramp import sensit_ramp
from core.Drivers.sensit_ramp_demo import sensit_ramp_demo
from core.Drivers.aurassure import aurassure
from core.Drivers.aeron import aeron 

driverList = {
    #"airveda" : AirVeda,
    "respirer": respirer,
    # "sensit_ramp": sensit_ramp,
    # "sensit_ramp_demo": sensit_ramp_demo,
    # "aurassure":aurassure,
    # "aeron":aeron
}


