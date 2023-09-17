

JAR="JavaFileServer-1.0-SNAPSHOT-jar-with-dependencies.jar"
JDIR="target/"
DIR="$(dirname $0)/$JDIR/$JAR"
CMD="java -jar $DIR"


CMD1="$CMD --Hcdn --Hextra config=SimpleCDN.json --Xdata server_dir"
CMD2="$CMD --Hcdn --Hextra config=SimpleCDN2.json --Xdata server_dir2"
CMD3="$CMD --Hcdn --Hextra config=SimpleCDN3.json --Xdata server_dir3"
CMD4="$CMD --Hcdn --Hextra config=SimpleCDN4.json --Xdata server_dir4"
CMD5="$CMD --Hcdn --Hextra config=SimpleCDN5.json --Xdata server_dir5"


tmux new-session -d -y 512 -x 64 "./ds 0" \;\
    set-option remain-on-exit failed \; \
    split-window "$CMD1" \;\
    split-window "$CMD2" \;\
    split-window "$CMD3" \;\
    split-window "$CMD4" \;\
    split-window "$CMD5" \;\
    select-layout tiled \;\
    attach
