import logging
from app.script.demi import ScriptDemi
from app.core.database import connect
from app.core import settings

if __name__ == "__main__":
    conn = connect()
    verbose=settings.VERBOSE
    logging.basicConfig(level=logging.DEBUG if verbose else logging.WARNING)
    script = ScriptDemi(connection=conn, verbose=verbose, ftp=False)
    #1: buscar los afifos en core
    #2: buscar los afifos en CSS
    #3: los afifos que estan en css pero no en core, cargar a core con todos sus datos
    #4: los afifos que estan en ambas separarlos en los que tienen diferencias (la info de un afifo en core puede estar desactualizada)
    #5: los afifos que tienen data vieja en core hay que actualizarlos con la data nueva de CSS
    old = script.load_old_data()
    new = script.load_new_data()
    script.compare_data(old, new)
