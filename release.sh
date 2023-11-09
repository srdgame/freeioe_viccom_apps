RELEASE_SCRIPT=$1
RELEASE_SUB=$2

RELEASE_APP (){
	$RELEASE_SCRIPT $RELEASE_SUB $1
}

RELEASE_APP Net_info
RELEASE_APP S7-200smart
RELEASE_APP Port_tunnel
RELEASE_APP toThingsBoard
RELEASE_APP _Customer_Projects/wicsun.com/yanli_delta_byc
RELEASE_APP _Customer_Projects/wicsun.com/yanli_delta_ccd
