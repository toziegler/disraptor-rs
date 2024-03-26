settings({
	nodaemon = true,
})

hosts = {
	{ ip = "genoa", port = 22 },
	{ ip = "c06", port = 22 },
	{ ip = "c08", port = 22 },
}

local filter = {
	"- *.csv",
	"- /target",
}

local function findGitignoreFilters()
	local cmd = "find . -type f -name '.gitignore'"
	local p = io.popen(cmd)
	local filters = {}
	for line in p:lines() do
		-- Format the path to be relative to the rsync source directory and prepend with "--filter=:- "
		local filter = "--filter=:- " .. line
		table.insert(filters, filter)
	end
	p:close()
	return filters
end

local targetdir = "./" .. io.popen("pwd"):read():match("([^/]-)$")
local gitignoreFilters = findGitignoreFilters()

for _, host in ipairs(hosts) do
	sync({
		default.rsyncssh,
		source = ".",
		targetdir = targetdir,
		host = host.ip,
		delay = 0,
		ssh = {
			port = host.port,
		},
		rsync = {
			perms = true,
			_extra = { table.unpack(gitignoreFilters) },
		},
		filter = filter,
	})
end
