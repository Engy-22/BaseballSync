from utilities.database.migrate import submit
from utilities.properties import import_driver_logger


def auto_migrate():
    import_driver_logger.log("\tTransferring all sandbox data to production environment")
    submit({'baseballData': {True: ["All"]},
            'pitch_fx': {True: ["All"]}}, True, True)
