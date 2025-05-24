SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create proc sys.sp_invoke_external_rest_endpoint
(
	@url nvarchar(4000),
	@payload nvarchar(max) = null,
	@headers nvarchar(4000) = null,
	@method nvarchar(6) = N'POST',
	@timeout smallint = 30,
	@credential sysname = null,
	@response nvarchar(max) = null output
)
as
begin
	set nocount on

	-- @headers is required to be a valid JSON
	if ISJSON(@headers) = 0
	begin
		raiserror(11558, 16, 200, 16, '@headers')
		return 1
	end

	declare @methodUpper nvarchar(6) = UPPER(@method collate Latin1_General_100_CI_AS)

	declare @returncode int = 0;

	BEGIN TRY
	-- Extract request and respone payload types as JSON, XML, or TEXT.
	declare @request_payload_type tinyint = 0;
	declare @response_payload_type tinyint = 0;

	SELECT @request_payload_type = sys.fn_extract_external_rest_endpoint_request_payload_type(@headers);
	SELECT @response_payload_type = sys.fn_extract_external_rest_endpoint_response_payload_type(@headers);

	-- Validate the @payload based on the detected request payload type
	-- JSON
	if @request_payload_type = 1 and ISJSON(@payload) = 0
	begin
		raiserror(11558, 16, 201, 16, '@payload')
		return 1
	end
	-- XML
	else if @request_payload_type = 2 AND @payload IS NOT NULL
	begin
		if TRY_CAST(@payload AS XML) IS NULL
		begin
			begin try
				-- To get the detailed error message from the XML parser,
				-- cast to XML in a try...catch block.
				select CAST(@payload AS XML)
			end try
			begin catch
				declare @err_msg1 nvarchar(max) = error_message();
				declare @err_msg1_len int = datalength(@err_msg1);
				raiserror(31640, 16, 1, 16, '@payload', @err_msg1_len, @err_msg1);
				return 1
			end catch
		end

		-- Remove the XML declaration from the request payload if it is present.
		-- This is in preparation for re-encoding the payload as UTF-8.
		-- The re-encoding occurs later in the workflow when the payload is transmitted to the endpoint.
		SELECT @payload = sys.fn_remove_external_rest_endpoint_xml_declaration(@payload);
	end

	declare @http_response_code int = null;
	declare @http_response_description nvarchar(4000) = null;
	declare @http_response_headers nvarchar(max) = null;
	declare @result nvarchar(max) = null;

	EXEC @returncode = sys.sp_invoke_external_rest_endpoint_internal @url, @payload, @request_payload_type, @response_payload_type, @headers, @methodUpper, @timeout, @credential, @http_response_code output, @http_response_description output, @http_response_headers output, @result output

	-- If invoking the external rest endpoint was successful, @result is required to be valid w.r.t. the desired response payload type.
	-- Else @result is evaluated, and if its format isn't valid, it's omitted from the @response by being overridden with a null value
	if @returncode = 0
	begin
		-- Invoking the external rest endpoint was successful. @result is required to be valid
		-- JSON
		if @response_payload_type = 1 and ISJSON(@result) = 0
		begin
			raiserror(11558, 16, 202, 14, '@result')
			return 1
		end
		-- XML
		else if @response_payload_type = 2 AND @result IS NOT NULL
		begin
			-- Remove the XML declaration from the response payload if it is present.
			-- It's possible that the result was read from the endpoint in a UTF-8 encoding and later re-encoded to UTF-16.
			-- If so, an XML declaration might no longer be accurate and could interfere with the XML's validity.
			SELECT @result = sys.fn_remove_external_rest_endpoint_xml_declaration(@result);

			if TRY_CAST(@result AS XML) IS NULL
			begin
				begin try
					-- To get the detailed error message from the XML parser,
					-- cast to XML in a try...catch block.
					select CAST(@result AS XML)
				end try
				begin catch
					declare @err_msg2 nvarchar(max) = error_message();
					declare @err_msg2_len int = datalength(@err_msg2);
					raiserror(31640, 16, 2, 14, '@result', @err_msg2_len, @err_msg2);
					return 1
				end catch
			end
		end
	end
	else
	begin
		-- Invoking the external rest endpoint was failed. if @result is invalid, override it with null
		-- JSON
		if @response_payload_type = 1 and ISJSON(@result) = 0
		begin
			set @result = null;
		end
		-- XML
		else if @response_payload_type = 2 AND @result IS NOT NULL
		begin
			-- Remove the XML declaration from the response payload if it is present.
			-- It's possible that the result was read from the endpoint in a UTF-8 encoding and later re-encoded to UTF-16.
			-- If so, an XML declaration might no longer be accurate and could interfere with the XML's validity.
			SELECT @result = sys.fn_remove_external_rest_endpoint_xml_declaration(@result);

			if TRY_CAST(@result AS XML) IS NULL
			begin
				set @result = null;
			end
		end
	end

	-- Construct the @response
	-- JSON or TEXT
	if @response_payload_type = 1 or @response_payload_type = 3
	begin
		-- Construct the @response template as a JSON
		-- This is used both for JSON and TEXT response payload types.
		declare @responseTemplateInJson nvarchar(max) = '{"response":{"status":{"http":{"code":null,"description":null}},"headers":null},"result":null}';

		-- If the response payload is JSON, convert the result to a JSON object.
		if @response_payload_type = 1
		begin
			set @response = JSON_MODIFY(@responseTemplateInJson, '$.result', JSON_QUERY(@result));
		end
		else if @response_payload_type = 3 -- Text
		begin
			set @response = JSON_MODIFY(@responseTemplateInJson, '$.result', @result);
		end

		set @response = JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@response
			, '$.response.status.http.code', @http_response_code)
			, '$.response.status.http.description', @http_response_description)
			, '$.response.headers', JSON_QUERY(@http_response_headers)); 
	end
	-- XML
	else if @response_payload_type = 2
	begin
		declare @headersXml xml = @http_response_headers
		declare @resultXml xml = @result

		declare @output as xml = (
			select
				http_status_code as 'response/status/http/@code',
				http_status_description as 'response/status/http/@description',
				http_response_headers as 'response/headers',
				@resultXml as 'result'
			from
				(values(
					@http_response_code,
					@http_response_description,
					@headersXml,
					@resultXml
				)) as [output](http_status_code, http_status_description, http_response_headers, result)
			for xml
				path(''), root('output'), type
		)

		set @response = cast(@output as nvarchar(max))
	end
	END TRY
	BEGIN CATCH
		THROW
	END CATCH

	return @returncode
end


/*====  SQL Server 2022 version  ====*/
create proc sys.sp_invoke_external_rest_endpoint
(
	@url nvarchar(4000),
	@payload nvarchar(max) = null,
	@headers nvarchar(4000) = null,
	@method nvarchar(6) = N'POST',
	@timeout smallint = 30,
	@credential sysname = null,
	@response nvarchar(max) = null output
)
as
begin
	set nocount on

	if ISJSON(@headers) = 0
	begin
		raiserror(11558, -1, 200, 16, '@headers')
		return 1
	end

	if ISJSON(@payload) = 0
	begin
		raiserror(11558, -1, 201, 16, '@payload')
		return 1
	end

	declare @methodUpper nvarchar(6) = UPPER(@method)

	declare @returncode int = 0;

	BEGIN TRY
		declare @http_response_code int = null;
		declare @http_response_description nvarchar(4000) = null;
		declare @http_response_headers nvarchar(max) = null;
		declare @result nvarchar(max) = null;

		EXEC @returncode = sys.sp_invoke_external_rest_endpoint_internal @url, @payload, @headers, @methodUpper, @timeout, @credential, @http_response_code output, @http_response_description output, @http_response_headers output, @result output

		-- If invoking the external rest endpoint was successful, @result is required to be a valid JSON.
		-- Else @result is evaluated, and if it's not a valid JSON it's omitted from the @response
		-- (by being overridden with a null value)
		if @returncode = 0
		begin
			if ISJSON(@result) = 0
			begin
				raiserror(11558, -1, 202, 14, '@result')
				return 1
			end
		end
		else
		begin
			if ISJSON(@result) = 0
			begin
				set @result = null;
			end
		end

		-- Construct the @response
		declare @responseTemplate nvarchar(max) = '{"response":{"status":{"http":{"code":null,"description":null}},"headers":null},"result":null}';

		/*
		Convert @http_response_headers which is in CRLF format (separated by \r\n) to json.
		Once we have the implementation of JSON_OBJECTAGG, the following T-SQL for the conversion can be simplified.
		1. Split headers by "\r\n" into key-value pair of header items. Keep the non-empty result in T(value)
		2. Using a cursor, fetch each row into @item and separate the key and value of the json.
		2.1. As header key cannot contain colon (:), it is safe to use first colon to separate the key and value in each header item.
			 Thus, if a colon exists, key and value are separated based on the first occurrence of the colon.
			 If there is no colon in the header item, the whole string is used as the key.
		2.2. The key and value are then appended to the @headersInJson within a try/catch block. If any error happens, the procedure continues
			 with the rest of header items but a warning message is generated.
		*/
		DECLARE @CrLf CHAR(2) = CHAR(13) + CHAR(10)
		DECLARE @headersInJson NVARCHAR(MAX)= N'{}', @item NVARCHAR(4000), @key NVARCHAR(4000), @value NVARCHAR(4000);
		DECLARE c CURSOR FOR
		 SELECT *
		   FROM STRING_SPLIT(REPLACE(@http_response_headers, @CrLf, '~'), '~', 0) T /* split by CRLF */
		   WHERE NULLIF(T.value, '') IS NOT NULL /* throw away empty rows */
		OPEN c;
		WHILE(1=1)
		BEGIN
			FETCH c INTO @item;
			IF @@FETCH_STATUS < 0 BREAK;
			
			-- Finding the index of first colon occurrence
			DECLARE @firstColonIndex int = CHARINDEX(':', @item, 0);
			
			-- If a colon exists, separate item into key and value based on the first colon occurrence
			IF @firstColonIndex > 0
			BEGIN
				SET @key = SUBSTRING(@item, 0, @firstColonIndex);
				SET @value = SUBSTRING(@item, @firstColonIndex + 1, LEN(@item));
			END
			ELSE
			BEGIN
				CONTINUE;
			END
			BEGIN TRY			
				SET @headersInJson = JSON_MODIFY(@headersInJson, CONCAT('$.', QUOTENAME(@key, '"')), TRIM(@value));
			END TRY
			BEGIN CATCH
				-- if due to any reason, a key value pair couldn't be parsed into the JSON,
				-- raise a warning message.
				DECLARE @msg nvarchar(4000) = N'Warning: unable to parse headers response "' + @key + '" into JSON';
				RAISERROR(@msg, 0,0) WITH NOWAIT
			END CATCH
		END;
		CLOSE c;
		DEALLOCATE c;

		set @response  = JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@responseTemplate
			, '$.response.status.http.code', @http_response_code)
			, '$.response.status.http.description', @http_response_description)
			, '$.response.headers', JSON_QUERY(@headersInJson))
			, '$.result', JSON_QUERY(@result));
	END TRY
	BEGIN CATCH
		THROW
	END CATCH

	return @returncode
end

