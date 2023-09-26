local Module = {}
local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")

local RetryDelay = 0.1
local MaxAttempts = 5

local function Retry(CallBack, FunctionName, RequestType, KeepTryingUntil)
	local Args = {pcall(CallBack)}
	if not Args[1] then
		local LimitReached = type(KeepTryingUntil) == "function" and KeepTryingUntil() == true or KeepTryingUntil == MaxAttempts
		if LimitReached then
			warn("Retry failed for:", FunctionName, "Message:", Args[2])
			return false
		else
			if RequestType then
				local CurrentBudget = DataStoreService:GetRequestBudgetForRequestType(RequestType) -- Handles throttling
				while CurrentBudget < 1 do
					CurrentBudget = DataStoreService:GetRequestBudgetForRequestType(RequestType)
					task.wait(RetryDelay+3)
				end
			else
				task.wait(RetryDelay)
			end
			
			local Attempt = (KeepTryingUntil or 1)
			if type(Attempt) ~= "function" then
				Attempt = Attempt+1
			end
			return Retry(CallBack, FunctionName, RequestType, Attempt)
		end
	else
		return unpack(Args)
	end
end

local function GetData(self, Index, KeepTryingUntilCallback)
	local Success,Value = Retry(function() 
		return self.DataStore:GetAsync(Index) 
	end, self.Name..":Get:"..tostring(Index), KeepTryingUntilCallback and Enum.DataStoreRequestType.GetAsync, KeepTryingUntilCallback)
	return Success,Value
end

local function SetData(self, Index, Value, KeepTryingUntilCallback)
	local Success,Value = Retry(function() 
		return self.DataStore:SetAsync(Index,Value) 
	end,self.Name..":Set:"..tostring(Index), KeepTryingUntilCallback and Enum.DataStoreRequestType.SetIncrementAsync, KeepTryingUntilCallback)
	return Success,Value
end

local function UpdateData(self, Index, Callback, KeepTryingUntilCallback)
	local Success,Value = Retry(function() 
		return self.DataStore:UpdateAsync(Index,Callback) 
	end,self.Name..":Update:"..tostring(Index), KeepTryingUntilCallback and Enum.DataStoreRequestType.UpdateAsync, KeepTryingUntilCallback)
	return Success,Value
end


if not RunService:IsStudio() then
	function Module.new(Name)
		local DataStoreTable = {}
		DataStoreTable.DataStore = DataStoreService:GetDataStore(Name)
		DataStoreTable.Name = Name
		
		DataStoreTable.Get = GetData
		DataStoreTable.Set = SetData
		DataStoreTable.Update = UpdateData
		
		return DataStoreTable
	end
else -- Add option to save
	local Nullary = function() 
		return nil 
	end
	
	function Module.new(Name)
		local DataStoreTable = {}
		DataStoreTable.DataStore = DataStoreService:GetDataStore(Name)
		DataStoreTable.Name = nil

		DataStoreTable.Get = Nullary
		DataStoreTable.Set = Nullary
		DataStoreTable.Update = Nullary

		return DataStoreTable
	end
end

return Module
