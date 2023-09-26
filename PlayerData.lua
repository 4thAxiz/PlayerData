local Module = {}
Module.PlayerData = {} -- {[Player] = {}}
Module.DefaultData = {
	["Level"] = 1,
	["EXP"] = 0,
	["Gold"] = 0,
	["Gems"] = 0,
}
Module.AutoSave = true

local AutoSaveFrequency = 120 -- (Seconds), should always be above 1 minute. Lowest allowable frequency is (60+PlayerCount*10)

local RunService = game:GetService("RunService")
local Utility = require(game.ReplicatedStorage:WaitForChild("Utility"))
local DataChangedRemote = script:WaitForChild("DataChanged")

function Module.GetData(Player) -- Accepts a Name or a Player, Or UserID
	Player = (type(Player) == "string" and game.Players[Player]) or (type(Player) == "number" and game.Players:GetPlayerByUserId(Player))

	if type(Player) == "userdata" and Player:IsA("Player") then
		while Module.PlayerData[Player] == nil do task.wait() end
		return Module.PlayerData[Player]
	else
		warn("Requested PlayerData with something that is not a Player:",Player)
	end
end

-------------------------------
-------------------------------
if RunService:IsServer() then--
-------------------------------
-------------------------------
	local DataStoreModule = require(script:WaitForChild("DataStore"))
	local PlayerDataStore = DataStoreModule.new("PlayerData01")
	local ServerJobID = RunService:IsStudio() and game:GetService("HttpService"):GenerateGUID(true) or game.JobId
	local DataVersion = 1
	local PlayerCount = 0
	Module.Tainted = {} -- Player = true -- This is true if a player's data couldn't load. Don't save, and don't sell this player anything


	local function DeserializeCyclicStringTable(StringTable, TablesEvaluted) -- Also unseralizes non-cyclic tables
		TablesEvaluted = TablesEvaluted or {}
		local DeserializedTable = {}
		local Key, Value, StartPosition, EndPosition, Character

		while #StringTable > 0 do
			StartPosition = string.find(StringTable, "%[") 
			EndPosition = string.find(StringTable,"%]=" ) 
			if StartPosition and EndPosition then
				Key = string.sub(StringTable, StartPosition+1, EndPosition-1) 
			else
				break
			end

			Character = string.sub(StringTable, EndPosition+2, EndPosition+2)
			if Character == "\"" then
				StartPosition = EndPosition + 3
				EndPosition = string.find(StringTable, "\"", StartPosition)
				Value = string.sub(StringTable, StartPosition, EndPosition-1)
			elseif Character == "{" then
				StartPosition = EndPosition + 2
				local count = 1
				for i = StartPosition, #StringTable do
					if string.sub(StringTable, i, i) == "{" then
						count = count + 1
					elseif string.sub(StringTable, i, i) == "}" then
						count = count - 1
					end

					if count == 0 then
						EndPosition = i
						break
					end
				end
				local InnerKeyString = string.sub(StringTable, StartPosition, EndPosition)
				Value = TablesEvaluted[InnerKeyString] or DeserializeCyclicStringTable("{" .. InnerKeyString .. "}", TablesEvaluted)
				TablesEvaluted[InnerKeyString] = Value
			else
				StartPosition = EndPosition + 2
				EndPosition = string.find(StringTable, ",", StartPosition) 
				if EndPosition then
					Value = string.sub(StringTable, StartPosition, EndPosition-1)
				else
					Value = string.sub(StringTable, StartPosition)
				end
			end

			DeserializedTable[Key] = Value
			StringTable = string.sub(StringTable, EndPosition+1)
		end

		return DeserializedTable
	end

	local function MakeDeserializable(Type)
		if type(Type) == "table" then
			if Type.X ~= nil and Type.Y ~= nil and Type.Z ~= nil then
				return Vector3.new(Type.X, Type.Y, Type.Z)
			elseif Type.Position ~= nil and Type.EulerAngleRepresentation ~= nil then
				local EulerAngles = MakeDeserializable(Type.EulerAngleRepresentation)
				return CFrame.fromOrientation(EulerAngles.X, EulerAngles.Y, EulerAngles.Z)+MakeDeserializable(Type.Position)
			elseif type(Type[1]) == "string" and string.match(Type[1], "^{.-}$") then -- Cyclic Table
				DeserializeCyclicStringTable(Type[1])
			else
				return Type -- Should be sanitized
			end
		else
			return Type
		end
	end

	local function MakeLoadable(Table)
		local Savable = {}
		for Index, Pair in Table do
			Savable[MakeDeserializable(Index)] = Savable[MakeDeserializable(Pair)]
			--Savable[Index] = MakeDeserializable(Index)
			--Savable[Pair] = MakeDeserializable(Pair)

			if type(Pair) == "table" then
				Savable[Index] = MakeLoadable(Pair)
			elseif type(Index) == "table" then
				Savable[Pair] = MakeLoadable(Index)
			else
				Savable[Index] = Pair
			end
		end

		return Savable
	end
	
	local function TaintedDataHandlerRetryCallback(Player)
		Module.Tainted[Player] = true
		return function() return  game.Players:FindFirstChild(Player) == nil end -- (Keeps retrying until)
	end
	
	function Module.LoadData(Player)
		local Success, Data = PlayerDataStore:Get(Player.UserId, TaintedDataHandlerRetryCallback(Player)); MakeLoadable(Data)
		if Success then
			if Data == nil or Data.Version == nil then -- New Player
				Data = Utility.TableCopy(Module.DefaultData)
				Data.Version = DataVersion
				Data.SessionData = {JobID = ServerJobID, TimeStamp = os.time()}
				return Data
			else
				if Data.SessionData == nil then
					Data.SessionData = {JobID = ServerJobID, TimeStamp = os.time()}
					Data.Version = DataVersion
				else -- Red flag
					local SessionAge = os.time() - Data.SessionData.TimeStamp
					if SessionAge >= AutoSaveFrequency then -- Has to be autosave frequency or it maay save between the time
						Data.SessionData = {JobID = ServerJobID, TimeStamp = os.time()}
						Data.Version = DataVersion
					else -- Trying to exploit duplication
						warn(Player, "is trying to exploit session locking. Loaded as tainted data...")
						Module.Tainted[Player] = true
					end
					warn(Player, "is potentially trying to exploit session locking")
				end
			end
			for Index, Value in Module.DefaultData do
				if Data[Index] == nil then
					Data[Index] = type(Value) == "table" and Utility.TableCopy(Value) or Value
				end
			end
			task.wait(); Module.Tainted[Player] = nil
		else
			warn("Something went wrong on Roblox's end... Could not load PlayerData for:", Player)
			Module.Tainted[Player] = true
		end

		Data.EXP += 1 -- Testing
		return Data
	end

	local function MakeSerializeable(Type, CyclicEvaluted) -- All that it should support right now
		if typeof(Type) == "Vector3" then
			return {X = Type.X, Y = Type.Y, Z = Type.Z} -- TODO: Add bitpacking
		elseif typeof(Type) == "CFrame" then
			local Position = Type.Position
			local EulerAngleRepresentation = Vector3.new(Type:ToEulerAnglesXYZ())
			return {Position = MakeSerializeable(Type), EulerAngleRepresentation = MakeSerializeable(Type),}
		elseif type(Type) == "table" and CyclicEvaluted ~= nil then -- Cyclic table type
			CyclicEvaluted = CyclicEvaluted or {}

			local SerializedCyclicTable = "{"
			for Key, Pair in Type do
				SerializedCyclicTable = SerializedCyclicTable.."[" .. tostring(Key) .. "]="
				if type(Pair) == "table" then
					if CyclicEvaluted[Pair] then
						SerializedCyclicTable = SerializedCyclicTable.."<cyclic>,"
					else
						CyclicEvaluted[Pair] = true
						SerializedCyclicTable = SerializedCyclicTable..MakeSerializeable(Pair, CyclicEvaluted)..","
					end
				elseif type(Pair) == "number" or type(Pair) == "boolean" then
					SerializedCyclicTable = SerializedCyclicTable..tostring(Pair) .. ","
				else
					SerializedCyclicTable = SerializedCyclicTable.."\""..tostring(Pair).."\","
				end
			end

			return SerializedCyclicTable .. "}"
		else -- No support for this type, fine as is
			return Type
		end
	end

	local function MakeSavable(Table, TablesEvaluated)
		TablesEvaluated = TablesEvaluated or {}
		if TablesEvaluated[Table] then
			warn("Cyclic Reference Detected In Data Table. Attempting to serialize: ", Table, debug.traceback())
			return { MakeSerializeable(Table, "CyclicTable") }
		end

		local Savable = {}
		for Index, Pair in Table do
			Savable[MakeSerializeable(Index)] = Savable[MakeSerializeable(Pair)]
			--Savable[Index] = MakeSerializeable(Index)
			--Savable[Pair] = MakeSerializeable(Pair)

			if type(Pair) == "table" then
				Savable[Index] = MakeSavable(Pair, TablesEvaluated)
			elseif type(Index) == "table" then
				Savable[Pair] = MakeSavable(Index, TablesEvaluated)
			else
				Savable[Index] = Pair
			end
		end
		TablesEvaluated[Table] = true
		return Savable
	end

	function Module.SaveData(Player, SessionEnd)
		local PlayerData = Module.PlayerData[Player]
		if PlayerData and Module.Tainted[Player] == nil then
			if PlayerData == nil then -- Rare case
				PlayerData.SessionData = not SessionEnd and {JobID = ServerJobID, TimeStamp = os.time()} or nil
			else
				local SessionData = PlayerData.SessionData
				if SessionData == nil then -- They are not in a session meaning this is safe to save, weird case.
					PlayerData.SessionData = not SessionEnd and {JobID = ServerJobID, TimeStamp = os.time()} or nil
				else -- Attempting to duplicate Data
					local JobID = SessionData.JobID
					local SessionAge = os.time() - SessionData.TimeStamp
					if JobID == ServerJobID then -- if it's from the same game instance then this is fine
						PlayerData.SessionData = not SessionEnd and {JobID = ServerJobID, TimeStamp = os.time()} or nil
					else  -- Trying to exploit duplication
						warn(Player, "is most likely trying to exploit session locking. Will not save data.")
						return false
					end
				end
			end

			local Success = PlayerDataStore:Set(Player.UserId, MakeSavable(PlayerData))
			if Success then
				return Success
			else
				warn("Unsucessful at saving", Player, "Data. Something went wrong on Roblox's end.")
			end
		else
			warn("Cannot save", Player, "data. Tainted Data...")

		end

		return false
	end

	local function FilterForNetworking(Table, Filter, FilteredTable)		
		if type(Filter) ~= "table" then -- key/pair filter -> pair/key
			for Key, Pair in Table do
				if Key == Filter then
					return {[Pair] = Key}
				elseif Pair == Filter then
					return {[Key] = Pair}
				else
					if type(Key) == "table" then
						local LinkedPair, LinkedKey = FilterForNetworking(Key, Filter)
						if LinkedPair then
							return {[LinkedPair] = LinkedKey}
						end
					end

					if type(Pair) == "table" then
						local LinkedPair, LinkedKey = FilterForNetworking(Pair, Filter)
						if LinkedPair then
							return {[LinkedPair] = LinkedKey}
						end
					end
				end
			end
		else
			FilteredTable = FilteredTable or {}
			for FilterKey, FilterValue in Filter do -- Accounts for mixed tables
				if FilterKey == nil then -- key/pair filter -> pair/key
					FilteredTable[FilterValue] = FilterForNetworking(Table, FilterValue, FilteredTable)
				else -- Key-pair filter
					for TableIndex, TableValue in Table do
						if TableIndex == FilterKey and TableValue == FilterValue then
							FilteredTable[TableIndex] = TableValue
							continue
						elseif type(TableIndex) == "table" and FilterForNetworking(FilterKey, FilterValue, FilteredTable) or (type(TableValue) == "table" and FilterForNetworking(FilterKey, FilterValue, FilteredTable)) then
							FilteredTable[TableIndex] = TableValue
						end
					end
				end
			end
		end
	end

	local function MakeNetworkable(Table)
		local NewTable = {}
		local Meta = getmetatable(Table)

		for Index,Value in Meta do
			if type(Index) == "number" then
				Index = tostring(Index)
			elseif type(Index) == "userdata" and game.Players:FindFirstChild(Index.Name) then
				Index = Index.Name
			end
			if type(Value) == "table" then
				NewTable[Index] = MakeNetworkable(Value)
			else
				NewTable[Index] = Value
			end
		end
		return NewTable
	end

	function Module.ReplicateData(Player, Filter, Recipients)
		local PlayerData = type(Player) == "table" and Player or Module.GetData(Player)
		if type(next(Filter)) == "userdata" then
			return warn("Did you accidentally send a list of player recipients in place of Filter? If you don't need a filter, mark this argument as nil.")
		end

		local NetworkableData = MakeNetworkable({[Player] = Filter and FilterForNetworking(PlayerData, Filter) or PlayerData}) -- Player should be replicated to the client at this point

		if Recipients == nil or type(Recipients) == "userdata" then
			DataChangedRemote:FireClient(Player, NetworkableData)
		elseif type(Recipients) == "string" and string.lower(Recipients) == "fireall" then
			DataChangedRemote:FireAllClients(NetworkableData)
		elseif Recipients == "table" then
			if #Recipients == 1 then
				DataChangedRemote:FireClient(Player, NetworkableData)
			else
				for _, Recipient in Recipients do
					DataChangedRemote:FireClient(Recipient, NetworkableData)
				end
			end
		else
			warn("Invalid Recipient type to send data to", Recipients, debug.traceback())
		end
	end

	-- 
	local function PlayerAdded(Player)
		if not RunService:IsStudio() then task.wait(5) end -- Delay for the ~4s DataStore cache delay (Prevents data exploitation via rapid server-changes)
		PlayerCount = PlayerCount + 1

		local Data = Module.LoadData(Player)
		Module.PlayerData[Player] = Data
		Module.ReplicateData(Player, Data)
	end

	game.Players.PlayerRemoving:Connect(function(Player)
		Module.SaveData(Player, true)
		Module.PlayerData[Player] = nil
		PlayerCount = PlayerCount - 1
	end)

	game.Players.PlayerAdded:Connect(PlayerAdded)
	for _,Player in game.Players:GetPlayers() do
		task.defer(PlayerAdded, Player)
	end

	if not RunService:IsStudio() then
		game:BindToClose(function()
			for Player,DataOfPlayer in Module.PlayerData do
				Module.SaveData(Player, true)
			end
		end)
	end
	-- 

	if Module.AutoSave then
		task.defer(function()
			while Module.AutoSave do
				for Player, PlayerData in Module.PlayerData do
					if PlayerData.LastSave == nil then
						PlayerData.LastSave = os.clock() 
					end
					if os.clock()-PlayerData.LastSave <= math.max(AutoSaveFrequency, 60+PlayerCount*10) then -- Must be under server limit.
						PlayerData.LastSave = os.clock()
						Module.SaveData(Player)
					end
				end
				task.wait(5)
			end
		end)
	end
------------------------------------
------------------------------------
elseif RunService:IsClient() then --
------------------------------------
------------------------------------
	local function ReceiveNetworkable(Table)
		local NewTable = {}
		for Index,Value in pairs(Table) do
			if type(Index) == "string" and game.Players:FindFirstChild(Index) then
				Index = game.Players:FindFirstChild(Index)
			end
			if type(Value) == "table" then
				NewTable[tonumber(Index) or Index] = ReceiveNetworkable(Value)
			else
				NewTable[tonumber(Index) or Index] = Value
			end
		end
		return NewTable
	end

	DataChangedRemote.OnClientEvent:Connect(function(RecievedData)
		for Player, PlayerData in ReceiveNetworkable(RecievedData) do --- {[Player] = PlayerData}
			Module.PlayerData[Player] = PlayerData
		end
	end)
end

return Module
