SHELL=/bin/bash

# window size(j) of parallels. refer manual of parallel command for detail.
N=40

# logs,  folder and jobs file.
baseDir=${PWD}
oDir=${baseDir}/logs-$(shell date +%Y%m%d-%H%M%S)
joblog=${oDir}/00-joblogs.txt

#
# in below, preproc creates argments for parallel command.
#
preproc=cat /dev/stdin
ifeq ($(shuf),true)               # when shuf is defined as true, apply shuf command
preproc +=| shuf
endif
ifneq ($(origin limit),undefined) # when limit is defined, apply it by head command.
preproc +=| head --lines ${limit}
endif


#stop to check
_stop_to_check:
	@echo ""
	@echo "preproc: ${preproc}"
	@echo "    env: ${env}"
	@echo "    cmd: ${cmd}"
	@echo "   args: ${args}"
	@echo "   oDir: ${oDir}"
	@echo ""
	@echo "how to use this makefile: pipe as below..."
	@echo ""
	@echo " cat dests.txt   | make -f executor.mk ping"
	@echo " cat dests.txt   | make -f executor.mk ping shuf=true limits=20"
	@echo ""
	@echo " cat dests.txt   | make -f executor.mk traceroute"
	@echo " cat dests.txt   | make -f executor.mk traceroute shuf=true limits=20"
	@echo ""
	@echo " * you can execute any command with parallel as below..."
	@echo " do some command | make -f executor.mk exec cmd=/usr/bin/... args='-opt1 val -opt2 val2 ...' env='LANG=C OTHERENV=BAR' "
	@echo ""

traceroute: ${oDir}
	$(eval cmd=traceroute)
	$(eval args=-I -n ${args})
	$(eval env=LANG=C ${env})
	${preproc} | parallel --eta -k -t -j ${N} --joblog ${joblog}    "sudo ${env} ${cmd} ${args} {}   1> >(tee ${oDir}/{} >&1) " || true

ping:  ${oDir}
	$(eval cmd=ping)
	$(eval args=-O -c 21 ${args})
	$(eval env=LANG=C ${env})
	${preproc} | parallel --eta -k -t -j ${N} --joblog ${joblog}    "${env} ${cmd} ${args} {}   1> >(tee ${oDir}/{} >&1) " || true

checkalives: ${oDir}
	$(eval cmd=ping)
	$(eval args=-O -c 3 ${args})
	$(eval env=LANG=C ${env})
	${preproc} | parallel --eta -k -t -j ${N} --joblog ${joblog}    "${env} ${cmd} ${args} {}   1> >(tee ${oDir}/{} >&1) " || true

exec:
ifneq ($(origin cmd),undefined) # only when cmd is defined in somehow...
	mkdir -p ${oDir}/stdout ${oDir}/stderr
	${preproc} | parallel --eta -k -t -j ${N} --joblog ${joblog}    "${env} ${cmd} ${args} {}   1> >(tee ${oDir}/stdout/{} >&1)  2> >(tee ${oDir}/stderr/{} >&2) " || true
else
	@echo 'required cmd is not given,  use make -f executor.mk exec cmd="..." '
endif


${oDir}:
	mkdir -p $@

_full_example:
	#cat somefile | parallel --eta -k -t -j ${N} --joblog joblog.txt     "somecmd {}       1> >(tee ${oDir}/stdout-{} >&1) 2> >(tee ${oDir}/stderr-{} >&2)" || true
