CREATE PROCEDURE ProcessDeviceEvent
    @DeviceID INT,
    @Temp FLOAT,
    @Humidity FLOAT,
    @EventTS DATETIME
AS
BEGIN
    DECLARE @HighTemp FLOAT, @LowTemp FLOAT, @HighHumidity FLOAT, @LowHumidity FLOAT
    DECLARE @HighTempDelay FLOAT, @LowTempDelay FLOAT, @HighHumidityDelay FLOAT, @LowHumidityDelay FLOAT
    DECLARE @StartTime DATETIME, @Delay FLOAT, @ExistingAlertCount INT

    -- Fetch device thresholds and delay times
    SELECT 
        @HighTemp = HighTemp, 
        @LowTemp = LowTemp, 
        @HighHumidity = HighHumidity, 
        @LowHumidity = LowHumidity,
        @HighTempDelay = HighTempDelay,
        @LowTempDelay = LowTempDelay,
        @HighHumidityDelay = HighHumidityDelay,
        @LowHumidityDelay = LowHumidityDelay
    FROM Devices_Threshold
    WHERE DeviceID = @DeviceID

    -- Insert the new event data into Devices_Data
    INSERT INTO Devices_Data (DeviceID, Temp, Humidity, EventTS)
    VALUES (@DeviceID, @Temp, @Humidity, @EventTS)

    -- Function to handle alert processing
    DECLARE @AlertType NVARCHAR(50)

    -- Reset StartTime if data normalize
    IF @Temp <= @HighTemp AND @Temp >= @LowTemp AND @Humidity <= @HighHumidity AND @Humidity >= @LowHumidity
    BEGIN
        DELETE FROM Device_Alert_Start
        WHERE DeviceID = @DeviceID
    END

    -- Process and insert High Temp alert
    IF @Temp > @HighTemp
    BEGIN
        SET @AlertType = 'High Temp'
        SET @Delay = @HighTempDelay
        EXEC HandleAlert @DeviceID, @AlertType, @EventTS, @Delay
    END

    -- Process and insert Low Temp alert
    IF @Temp < @LowTemp
    BEGIN
        SET @AlertType = 'Low Temp'
        SET @Delay = @LowTempDelay
        EXEC HandleAlert @DeviceID, @AlertType, @EventTS, @Delay
    END

    -- Process and insert High Humidity alert
    IF @Humidity > @HighHumidity
    BEGIN
        SET @AlertType = 'High Humidity'
        SET @Delay = @HighHumidityDelay
        EXEC HandleAlert @DeviceID, @AlertType, @EventTS, @Delay
    END

    -- Process and insert Low Humidity alert
    IF @Humidity < @LowHumidity
    BEGIN
        SET @AlertType = 'Low Humidity'
        SET @Delay = @LowHumidityDelay
        EXEC HandleAlert @DeviceID, @AlertType, @EventTS, @Delay
    END

    -- Declare table variable to capture resolved alerts
    DECLARE @ResolvedAlerts TABLE (DeviceID INT, AlramType NVARCHAR(50), Status NVARCHAR(10))

    -- Resolve existing alerts and capture the resolved alert types
    UPDATE Alarm
    SET Status = 'RESOLVED', ResolveEventTS = @EventTS
    OUTPUT INSERTED.DeviceID, INSERTED.AlramType, 'RESOLVED' INTO @ResolvedAlerts
    WHERE DeviceID = @DeviceID AND Status = 'OPEN'
    AND (
        (@Temp <= @HighTemp AND AlramType = 'High Temp') OR
        (@Temp >= @LowTemp AND AlramType = 'Low Temp') OR
        (@Humidity <= @HighHumidity AND AlramType = 'High Humidity') OR
        (@Humidity >= @LowHumidity AND AlramType = 'Low Humidity')
    )

    IF (SELECT COUNT(*) FROM @ResolvedAlerts) > 0
    BEGIN

        SELECT DeviceID, AlramType, Status FROM @ResolvedAlerts
    END
END

GO

-- Helper procedure to handle individual alert processing
CREATE PROCEDURE HandleAlert
    @DeviceID INT,
    @AlertType NVARCHAR(50),
    @EventTS DATETIME,
    @Delay FLOAT
    
AS
BEGIN
    DECLARE @StartTime DATETIME, @ExistingAlertCount INT, @Status NVARCHAR(10)

    -- Check if an alert start time exists for this type
    SELECT @StartTime = StartTime
    FROM Device_Alert_Start
    WHERE DeviceID = @DeviceID AND AlertType = @AlertType

    IF @StartTime IS NULL
    BEGIN
        -- Insert a new start time if not already existing
        INSERT INTO Device_Alert_Start (DeviceID, AlertType, StartTime)
        VALUES (@DeviceID, @AlertType, @EventTS)
    END
    ELSE IF DATEDIFF(MINUTE, @StartTime, @EventTS) >= @Delay
    BEGIN
        -- Check if the alert already exists
        SELECT @ExistingAlertCount = COUNT(*)
        FROM Alarm
        WHERE DeviceID = @DeviceID AND AlramType = @AlertType AND Status = 'OPEN'

        -- Insert alert if no existing one
        IF @ExistingAlertCount = 0
        BEGIN
            INSERT INTO Alarm (DeviceID, AlramType, Status, EventTS)
            VALUES (@DeviceID, @AlertType, 'OPEN', @EventTS)

            DECLARE @AlertDetails TABLE (DeviceID INT, AlramType NVARCHAR(50), Status NVARCHAR(10))

            INSERT INTO @AlertDetails VALUES (@DeviceID, @AlertType, 'OPEN')
            SELECT DeviceID, AlramType, Status FROM @AlertDetails

            -- Remove the start time entry
            DELETE FROM Device_Alert_Start
            WHERE DeviceID = @DeviceID AND AlertType = @AlertType
        END
    END
END

GO
