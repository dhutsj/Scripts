#!/bin/sh

if [ "X$1" = "X" ]; then
    echo "Usage: fds-component.sh [list|start|stop|unload|delete|get|set]"
    echo "       fds-ctrl.sh list"
    echo "       fds-ctrl.sh <start|stop|unload|delete> <Component>"
    echo "       fds-ctrl.sh get <MO> <Component[/Version[/Host[/Instance]]]>"
    echo "       fds-ctrl.sh set <MO> <Component[/Version[/Host[/Instance]]]> <Parameter> <Value>"
    exit 1
fi

fdsPrintError()
{
    echo $1 | grep '<Response>' > /dev/null
    if [ $? -ne 0 ]; then
        echo "$2""MO Request failure" >&2
        return 1
    fi
    reason=`echo $1 | grep -o '<Error>.*</Error>'`
    if [ $? -eq 0 ]; then
        echo "$2"`echo $reason | grep -o '<Reason>.*</Reason>' | sed -e 's%<Reason>\(.*\)</Reason>%\1%' | sed  -e 's/&quot;/"/g' -e "s/&apos;/'/g" -e 's/&lt;/</g' -e 's/&gt;/>/g' -e 's/&amp;/&/g'` >&2
        return 1
    fi

    return 0
}

fdsList()
{
    resp=`echo '<Request MO=FDSController Operation=Get SessionId=0><ComponentList></ComponentList></Request>' | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
    fdsPrintError "$resp" "Get component list failure: "
    if [ $? -eq 1 ]; then
        return 1
    fi
    
    printf '%-50s %-20s\n' 'Component' 'State'
    echo ==========================================================
    echo $resp | grep -o '<LogicalComponent[ \t][ \t]*[^>]*>[^<]*</LogicalComponent>' | sed 's/<LogicalComponent[ \t][ \t]*Name="\([^"]*\)"[ \t][ \t]*Host="[^"]*"[ \t][ \t]*CurrentState="FDSPLUGIN_STATE_\([A-Z_]*\)".*/\1 \2/' | awk '{ printf("%-50s %-20s\n", $1, $2); }'
    return 0
}

fdsCtrl()
{
    operation=$1

    resp=`echo '<Request MO=FDSController Operation=Get SessionId=0><ComponentList></ComponentList></Request>' | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
    fdsPrintError "$resp" "Get component list failure: "
    if [ $? -eq 1 ]; then
        return 1
    fi

    components=`echo $resp | grep -o '<LogicalComponent[ \t][ \t]*[^>]*>[^<]*</LogicalComponent>' | sed 's/<LogicalComponent Name="\([^"]*\)".*/\1/g' | grep $2`
    if [ "X$components" = "X" ]; then
        echo "Invalid component name" >&2
        return 1
    fi

    for i in $components; do
        resp=`echo "<Request MO=FDSController Operation=$operation SessionId=0><LogicalComponent>$i</LogicalComponent></Request>" | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
        fdsPrintError "$resp" "$1 $i failure: "
    done

    return 0
}

fdsGet()
{
    mo=$1
    component=$2
    resp=`echo "<Request MO=\"$mo\" Operation=GetConfig SessionId=0><RuntimeConfiguration LogicalComponent=\"$component\"/></Request>" | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
    fdsPrintError "$resp"
    if [ $? -eq 1 ]; then
        return 1
    fi

    printf '%-8s%-40s%-8s%-7s%-13s%-s\n' 'Type' 'Name' 'Runtime' 'Hidden' 'Configration' 'Value'
    echo ======================================================================================
    echo $resp | grep -o '<[a-zA-Z]*Parameter[ \t][^>]*>[^<]*</[a-zA-Z]*Parameter>' | sed -e 's%<\([a-zA-Z]*\)Parameter[ \t][ \t]*Name="\([^"]*\)"[ \t][ \t]*Time="[^"]*"[ \t][ \t]*UTCTime="[^"]*"[ \t][ \t]*RuntimeConfigurable="\([^"]*\)"[ \t][ \t]*Hidden="\([^"]*\)"[ \t][ \t]*ConfigurationType="\([^"]*\)"[ \t]*>\([^<]*\)</[a-zA-Z]*Parameter>%\1\t\2\t\3\t\4\t\5\t\6%g' | awk -F $'\t' '{ printf("%-8s%-40s%-8s%-7s%-13s%-s\n", $1, $2, ($3 == 1) ? "yes" : "no", ($4 == 1) ? "yes": "no", $5, $6); }'
    return 0
}

fdsSet()
{
    mo=$1
    component=$2
    name=$3
    value=$4
    resp=`echo '<Request MO="'$mo'" Operation=GetConfig SessionId=0><RuntimeConfiguration LogicalComponent="'$component'"/></Request>' | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
    fdsPrintError "$resp" "Get component configuration failure: "
    if [ $? -eq 1 ]; then
        return 1
    fi

    req=`echo $resp | grep -o "<[a-zA-Z]*Parameter[ \t][ \t]*Name=\"$name\"[^>]*>[^<]*</[a-zA-Z]*Parameter>" | sed "s%\(<[a-zA-Z]*Parameter[ \t][ \t]*[^>]*>\)\([^<]*\)\(</[a-zA-Z]*Parameter>\)%\1$value\3%"`
    if [ "X$req" == "X" ]; then
        echo "Get component configuration failure: Invalid parameter"
    fi

    resp=`echo '<Request MO="'$mo'" Operation=SetConfig SessionId=0><RuntimeConfiguration LogicalComponent="'$component'">'$req'</RuntimeConfiguration></Request>' | /opt-mpc/FDS/bin/FDSRequestSender -u Setup -p 'Earth_#10' - 2>/dev/null`
    fdsPrintError "$resp" "Set component configuration failure: "
    if [ $? -eq 1 ]; then
        return 1
    fi

    return 0
}

operation=`echo $1 | sed -e 's/.*/\L&/g' -e 's/\b[a-z]/\U&/g'`
case $operation in
    List)
        fdsList
        exit $?
        ;;
    Load|Start|Stop|Unload|Delete)
        shift
        while [ "X$1" != "X" ]; do
            fdsCtrl $operation $1
            shift
        done
        ;;
    Get)
        fdsGet $2 $3
        exit $?
        ;;
    Set)
        fdsSet $2 $3 $4 $5
        exit $?
        ;;
    *)
        echo "Unknown command"
        ;;
esac

