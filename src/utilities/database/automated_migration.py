from utilities.database.migrate import submit
from utilities.properties import import_driver_logger


def auto_migrate():
    import_driver_logger.log("\tTransferring all sandbox data to production environment\n")
    submit({'baseballData': {True: ["All"]},
            'pitchers_pitch_fx': {True: ["All"]},
            'batters_pitch_fx': {True: ["All"]}}, True, True)
