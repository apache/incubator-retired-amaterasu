#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

export AMA_NODE="$(hostname)"
#pushd $BASEDIR >/dev/null
#cd /mesos-dependencies/ && nohup java -cp ${BASEDIR}/amaterasu-assembly-0.1.0.jar -Djava.library.path=/usr/lib io.shinto.amaterasu.utilities.HttpServer &
#SERVER_PID=$!



echo "serving amaterasu from /ama/lib on user supplied port"
popd >/dev/null

RED=`tput setaf 1`
YELLOW=`tput setaf 3`
NC=`tput sgr0`
bold=$(tput bold)
normal=$(tput sgr0)

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    export AWS_ACCESS_KEY_ID=0
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    export AWS_SECRET_ACCESS_KEY=0
fi

echo ""
echo ""
echo "${bold}${RED}                                             /\ "
echo "  				            /  \ /\ "
echo "  				           / ${YELLOW}/\\${RED} /  \ "
echo "      ${NC}${bold}_                 _               ${RED}  / ${YELLOW}/${RED}  / ${YELLOW}/\\${RED} \ ${NC}"
echo "${bold}     /_\   _ __   __ _ | |_  ___  _ _  __${RED}(${NC}${bold}_${YELLOW}( ${NC}${bold}_${RED}(${NC}${bold}_${YELLOW}(${NC}${bold}_ ${YELLOW})${NC}${bold}_${RED})${NC}${bold} "
echo "    / _ \ | '  \ / _\` ||  _|/ -_)| '_|/ _\` |(_-<| || | "
echo "   /_/ \_\|_|_|_|\__,_| \__|\___||_|  \__,_|/__/ \_,_| "
echo ""
echo "    Continuously deployed data pipelines"
echo "    Version 0.2.0"
echo "${NC}"
echo ""

for i in "$@"
do
case $i in
    -r=*|--repo=*)
    REPO="${i#*=}"
    shift # past argument=value
    ;;
    -b=*|--branch=*)
    BRANCH="${i#*=}"
    shift # past argument=value
    ;;
    -e=*|--env=*)
    ENV="${i#*=}"
    shift # past argument=value
    ;;
    -n=*|--name=*)
    NAME="${i#*=}"
    shift # past argument=value
    ;;
    -i=*|--job-id=*)
    JOBID="${i#*=}"
    shift # past argument=value
    ;;
    -i=*|--report=*)
    REPORT="${i#*=}"
    shift # past argument=value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument with no value
    ;;
    *)
            # unknown option
    ;;
esac
done

echo "repo: ${REPO} "
CMD="java -cp ${BASEDIR}/bin/*.jar -Djava.library.path=/usr/lib io.shinto.amaterasu.leader.mesos.JobLauncher --home ${BASEDIR}" #--repo "https://github.com/roadan/amaterasu-job-sample.git" --branch master

if [ -n "$REPO" ]; then
    CMD+=" --repo ${REPO}"
fi

if [ -n "$BRANCH" ]; then
    CMD+=" --branch ${BRANCH}"
fi

if [ -n "$ENV" ]; then
    CMD+=" --env ${ENV}"
fi

if [ -n "$NAME" ]; then
    CMD+=" --name ${NAME}"
fi

if [ -n "$JOBID" ]; then
    CMD+=" --job-id ${JOBID}"
fi

if [ -n "$REPORT" ]; then
    CMD+=" --report ${REPORT}"
fi

echo $CMD

eval $CMD | grep "===>"
kill $SERVER_PID

echo ""
echo ""
echo "W00t amaterasu job is finished!!!"
