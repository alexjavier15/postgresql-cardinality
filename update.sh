#/bin/sh

scp	/usr/local/pgsql/data/explain.jar arivas@diascld17.epfl.ch:/home_local/rivas/pgsql/data/explain.jar
scp   contrib/auto_explain/auto_explain.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/contrib/auto_explain/auto_explain.c 
scp   src/backend/optimizer/path/costsize.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/path/costsize.c
scp   src/backend/optimizer/path/indxpath.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/path/indxpath.c
scp   src/backend/optimizer/path/joinpath.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/path/joinpath.c
scp         src/backend/optimizer/path/joinrels.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/path/joinrels.c
scp           src/backend/optimizer/plan/createplan.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/plan/createplan.c
scp        src/backend/optimizer/plan/planmain.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/plan/planmain.c
scp         src/backend/optimizer/plan/planner.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/plan/planner.c
scp       src/backend/optimizer/util/pathnode.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/util/pathnode.c
scp           src/backend/optimizer/util/plancat.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/util/plancat.c
scp          src/backend/optimizer/util/relnode.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/optimizer/util/relnode.c
scp           src/backend/utils/misc/guc.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/utils/misc/guc.c
scp           src/include/nodes/plannodes.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/nodes/plannodes.h
scp          src/include/nodes/relation.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/nodes/relation.h
scp        src/include/optimizer/cost.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/optimizer/cost.h
scp       src/include/optimizer/pathnode.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/optimizer/pathnode.h
scp      src/include/nodes/outfuncs.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/nodes/outfuncs.h
scp  	 src/backend/nodes/outfuncs.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/nodes/outfuncs.c
scp   src/backend/commands/explain.c arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/backend/commands/explain.c
scp    src/include/commands/prepare.h arivas@diascld17.epfl.ch:/home_local/rivas/postgresql/src/include/commands/prepare.h
