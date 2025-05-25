use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create function sys.fn_MSrepl_map_resolver_clsid (
    @compatibility_level  int,                      /* use 70 as the default compatibility level */
    @article_resolver  nvarchar(255),               /* article resolver name for verification purposes */
    @resolver_clsid  nvarchar(60)                   /* resolver class ID to be mapped */
    ) returns nvarchar(60)
    AS
    begin

    declare @additive_resolver_clsid    nvarchar(60)
    declare @average_resolver_clsid     nvarchar(60)
    declare @download_resolver_clsid    nvarchar(60)
    declare @max_resolver_clsid         nvarchar(60)
    declare @mergetxt_resolver_clsid    nvarchar(60)
    declare @min_resolver_clsid         nvarchar(60)
    declare @subwins_resolver_clsid     nvarchar(60)
    declare @upload_resolver_clsid      nvarchar(60)
    declare @sp_resolver_clsid          nvarchar(60)

    declare @160additive_resolver_clsid nvarchar(60)
    declare @160average_resolver_clsid  nvarchar(60)
    declare @160download_resolver_clsid nvarchar(60)
    declare @160max_resolver_clsid      nvarchar(60)
    declare @160mergetxt_resolver_clsid nvarchar(60)
    declare @160min_resolver_clsid      nvarchar(60)
    declare @160subwins_resolver_clsid  nvarchar(60)
    declare @160upload_resolver_clsid   nvarchar(60)
    declare @160sp_resolver_clsid       nvarchar(60)

    declare @150additive_resolver_clsid nvarchar(60)
    declare @150average_resolver_clsid  nvarchar(60)
    declare @150download_resolver_clsid nvarchar(60)
    declare @150max_resolver_clsid      nvarchar(60)
    declare @150mergetxt_resolver_clsid nvarchar(60)
    declare @150min_resolver_clsid      nvarchar(60)
    declare @150subwins_resolver_clsid  nvarchar(60)
    declare @150upload_resolver_clsid   nvarchar(60)
    declare @150sp_resolver_clsid       nvarchar(60)

	declare @140additive_resolver_clsid nvarchar(60)
    declare @140average_resolver_clsid  nvarchar(60)
    declare @140download_resolver_clsid nvarchar(60)
    declare @140max_resolver_clsid      nvarchar(60)
    declare @140mergetxt_resolver_clsid nvarchar(60)
    declare @140min_resolver_clsid      nvarchar(60)
    declare @140subwins_resolver_clsid  nvarchar(60)
    declare @140upload_resolver_clsid   nvarchar(60)
    declare @140sp_resolver_clsid       nvarchar(60)

    declare @130additive_resolver_clsid nvarchar(60)
    declare @130average_resolver_clsid  nvarchar(60)
    declare @130download_resolver_clsid nvarchar(60)
    declare @130max_resolver_clsid      nvarchar(60)
    declare @130mergetxt_resolver_clsid nvarchar(60)
    declare @130min_resolver_clsid      nvarchar(60)
    declare @130subwins_resolver_clsid  nvarchar(60)
    declare @130upload_resolver_clsid   nvarchar(60)
    declare @130sp_resolver_clsid       nvarchar(60)

    declare @120additive_resolver_clsid nvarchar(60)
    declare @120average_resolver_clsid  nvarchar(60)
    declare @120download_resolver_clsid nvarchar(60)
    declare @120max_resolver_clsid      nvarchar(60)
    declare @120mergetxt_resolver_clsid nvarchar(60)
    declare @120min_resolver_clsid      nvarchar(60)
    declare @120subwins_resolver_clsid  nvarchar(60)
    declare @120upload_resolver_clsid   nvarchar(60)
    declare @120sp_resolver_clsid       nvarchar(60)

    declare @110additive_resolver_clsid nvarchar(60)
    declare @110average_resolver_clsid  nvarchar(60)
    declare @110download_resolver_clsid nvarchar(60)
    declare @110max_resolver_clsid      nvarchar(60)
    declare @110mergetxt_resolver_clsid nvarchar(60)
    declare @110min_resolver_clsid      nvarchar(60)
    declare @110subwins_resolver_clsid  nvarchar(60)
    declare @110upload_resolver_clsid   nvarchar(60)
    declare @110sp_resolver_clsid       nvarchar(60)

    declare @100additive_resolver_clsid nvarchar(60)
    declare @100average_resolver_clsid  nvarchar(60)
    declare @100download_resolver_clsid nvarchar(60)
    declare @100max_resolver_clsid      nvarchar(60)
    declare @100mergetxt_resolver_clsid nvarchar(60)
    declare @100min_resolver_clsid      nvarchar(60)
    declare @100subwins_resolver_clsid  nvarchar(60)
    declare @100upload_resolver_clsid   nvarchar(60)
    declare @100sp_resolver_clsid       nvarchar(60)

    declare @90additive_resolver_clsid  nvarchar(60)
    declare @90average_resolver_clsid   nvarchar(60)
    declare @90download_resolver_clsid  nvarchar(60)
    declare @90max_resolver_clsid       nvarchar(60)
    declare @90mergetxt_resolver_clsid  nvarchar(60)
    declare @90min_resolver_clsid       nvarchar(60)
    declare @90subwins_resolver_clsid   nvarchar(60)
    declare @90upload_resolver_clsid    nvarchar(60)
    declare @90sp_resolver_clsid        nvarchar(60)

    declare @80additive_resolver_clsid  nvarchar(60)
    declare @80average_resolver_clsid   nvarchar(60)
    declare @80download_resolver_clsid  nvarchar(60)
    declare @80max_resolver_clsid       nvarchar(60)
    declare @80mergetxt_resolver_clsid  nvarchar(60)
    declare @80min_resolver_clsid       nvarchar(60)
    declare @80subwins_resolver_clsid   nvarchar(60)
    declare @80upload_resolver_clsid    nvarchar(60)
    declare @80sp_resolver_clsid        nvarchar(60)

    select @additive_resolver_clsid = '{0D64B1B7-1E18-48CF-A7E8-7F6D9861DD05}'
    select @average_resolver_clsid  = '{A110D612-7FB7-4471-805D-0C4FD58403D3}'
    select @download_resolver_clsid = '{56B0953F-DDF6-423E-BC15-0CCE657088FA}'
    select @max_resolver_clsid      = '{915051D3-45C3-44A2-9EEC-3BA8FA575B7C}'
    select @mergetxt_resolver_clsid = '{3310B051-64FC-47C6-A7C2-03CB54BB8C54}'
    select @min_resolver_clsid      = '{8D22F39E-EEBF-4A2C-9698-8AA84152A2D2}'
    select @subwins_resolver_clsid  = '{20C8E8F2-1017-49E8-98E5-C143833D5626}'
    select @upload_resolver_clsid   = '{790DD78E-636F-4CA9-A6F9-AAB1EACCA3DB}'
    select @sp_resolver_clsid       = '{709A9DEE-97DA-4486-A479-B94EC8229D21}'

    select @160additive_resolver_clsid = '{6ACA9C22-3CC2-4947-9D5F-525A1F9E8B45}'
    select @160average_resolver_clsid  = '{F09F3613-7C9A-481F-952D-84B5E1060AC6}'
    select @160download_resolver_clsid = '{B4CA61C8-7495-4F5A-9EE1-AEAF685693C8}'
    select @160max_resolver_clsid      = '{608EB36F-373A-4485-A1AA-A21DE806FDF1}'
    select @160mergetxt_resolver_clsid = '{C6CE3676-53F4-47B5-B3AF-1BB44996192E}'
    select @160min_resolver_clsid      = '{CCC9BC97-EC98-4210-9BA0-2FE28C6DE077}'
    select @160subwins_resolver_clsid  = '{BB6E90FC-FFE5-4256-9906-56BDFD7F1CAB}'
    select @160upload_resolver_clsid   = '{482BCCD2-3FEB-4CA3-84C6-A380E3AB10E8}'
    select @160sp_resolver_clsid       = '{D2701A2D-9D79-41BC-B7C4-1F2B5CF891B7}'

    select @150additive_resolver_clsid  = '{D8A56ACA-5796-4701-8C06-87CC3B8A3BFD}'
    select @150average_resolver_clsid   = '{B1A99B7D-0CA5-4C47-B18A-EEF10D6D7B54}'
    select @150download_resolver_clsid  = '{9A14C179-42AE-4F99-B2EB-078F9892991E}'
    select @150max_resolver_clsid       = '{10D3B90F-EC51-495F-BC3F-74BFAB3850E9}'
    select @150mergetxt_resolver_clsid  = '{EFCA64BD-6289-4F30-8517-414190E2B5C8}'
    select @150min_resolver_clsid       = '{8D47A9E4-5EAE-40A1-90C7-CC099C165795}'
    select @150subwins_resolver_clsid   = '{EA19DB37-5534-4D93-95EF-70BA004DD29B}'
    select @150upload_resolver_clsid    = '{01F9206D-C615-4E1D-8C06-90A7A2A3F3A9}'
    select @150sp_resolver_clsid        = '{048131BB-09DD-4B6E-BF67-9B7347AD710F}'

	select @140additive_resolver_clsid = '{BFC711F4-5750-4CB3-B7FB-FDE31FC54DB3}'
    select @140average_resolver_clsid  = '{D466CB4D-A131-4067-984F-F1416BBECD5A}'
    select @140download_resolver_clsid = '{087ABE5E-9ABC-43F3-AD45-D38F0BBEA31D}'
    select @140max_resolver_clsid      = '{63114856-E965-4FAB-9D90-5756795FA484}'
    select @140mergetxt_resolver_clsid = '{C61365C1-437A-460C-92C7-3F06CAD44F22}'
    select @140min_resolver_clsid      = '{E76EED9D-B1F5-4E10-9CD4-7D7A63FAEDB7}'
    select @140subwins_resolver_clsid  = '{D45D6C0D-E6B2-4E0E-91C6-2E64A49757F2}'
    select @140upload_resolver_clsid   = '{F550DC00-B806-497A-99EC-636480D560D8}'
    select @140sp_resolver_clsid       = '{173648B0-37B3-4A3B-9BD3-8627E65C6EE5}'

    select @130additive_resolver_clsid = '{D2691D5C-F78E-4566-A818-B2A1092ACBEE}'
    select @130average_resolver_clsid  = '{C85D9955-8790-4DE6-9EB9-BE6A3FE72025}'
    select @130download_resolver_clsid = '{C688A401-46DD-4A4E-8B8E-C293A3E4D3FA}'
    select @130max_resolver_clsid      = '{9FEABCBE-50F2-4DC5-B323-E9E9A81B564E}'
    select @130mergetxt_resolver_clsid = '{F19522DD-0627-4AE3-8B8F-3C4363B3D4C8}'
    select @130min_resolver_clsid      = '{6EF1F47D-880B-42B3-A939-A5884202F3DE}'
    select @130subwins_resolver_clsid  = '{56604C05-9197-4D50-AF78-F45E201E495D}'
    select @130upload_resolver_clsid   = '{C0FCE632-D230-49FC-ACA0-968F20CD6D40}'
    select @130sp_resolver_clsid       = '{163EBC43-A5CA-4972-960A-0F2B1CE8D6EF}'

    select @120additive_resolver_clsid = '{408547ED-AA55-4077-AD6E-621B279DC81C}'
    select @120average_resolver_clsid  = '{0BEE7B25-09E9-4B74-AA8C-B306EA1C4072}'
    select @120download_resolver_clsid = '{66BAF4FC-ED0F-4479-9397-7FBCCFB3FAF2}'
    select @120max_resolver_clsid      = '{51F9F46D-7497-42F7-8F4D-E4B1724598AE}'
    select @120mergetxt_resolver_clsid = '{FE6E5C13-A27E-4BFD-A0B7-5B60017D20BF}'
    select @120min_resolver_clsid      = '{95EDC0AD-AA41-43F6-8F44-400633DFEF2E}'
    select @120subwins_resolver_clsid  = '{DBD898EC-A745-4992-BCAA-485161E736A9}'
    select @120upload_resolver_clsid   = '{33AFDE81-1461-41CF-9930-38E5AFFFC379}'
    select @120sp_resolver_clsid       = '{64C5CD68-54A3-4961-8053-445321C795E2}'
    
    select @110additive_resolver_clsid = '{8FE7FF34-7C5D-4BE7-8056-ADB6D6F692DC}'
    select @110average_resolver_clsid  = '{376F678E-4691-43E8-8AE7-DAD8CAA644EF}'
    select @110download_resolver_clsid = '{3BB9F418-3407-4F5B-8DB3-9E9147C3A710}'
    select @110max_resolver_clsid      = '{7365BF95-62E8-4B72-A0F7-E238FE413DB7}'
    select @110mergetxt_resolver_clsid = '{9DCD5250-86BB-433D-8C1F-561460105CF0}'
    select @110min_resolver_clsid      = '{93277AB4-C338-48B8-9A4A-CA5A32587AB7}'
    select @110subwins_resolver_clsid  = '{77E52C5E-0016-4EDF-9391-8C07BFB668CE}'
    select @110upload_resolver_clsid   = '{3D43EBE7-063C-4447-91E7-DE7A264C8441}'
    select @110sp_resolver_clsid       = '{3BB074FA-0836-4A63-BE0C-AF49DDD42A1C}'

    select @100additive_resolver_clsid = '{D2CCB059-65DD-497B-8822-7660B7778DDF}'
    select @100average_resolver_clsid  = '{91DD61BF-D937-4A21-B0EF-36204A328439}'
    select @100download_resolver_clsid = '{9602B431-2937-4D51-8CC3-11F8AC1EC26D}'
    select @100max_resolver_clsid      = '{77209412-47CF-49AF-A347-DCF7EE481277}'
    select @100mergetxt_resolver_clsid = '{0045200C-9126-4432-BC9B-3186D141EB5A}'
    select @100min_resolver_clsid      = '{2FF7564F-9D55-48C0-A4C1-C148076D9119}'
    select @100subwins_resolver_clsid  = '{E93406CC-5879-4143-B70B-29B385BA80C9}'
    select @100upload_resolver_clsid   = '{05614E0C-92A9-45F3-84A4-46C8E36424A9}'
    select @100sp_resolver_clsid       = '{D264B5C0-1300-471A-80C9-9C1FC34A3691}'

    select @90additive_resolver_clsid  = '{4B385BCE-190B-46c5-AEAB-51358A8E1CB6}'
    select @90average_resolver_clsid   = '{337754AA-CF6E-4be8-8F13-F6AC91524EDC}'
    select @90min_resolver_clsid       = '{464CBD74-3177-4593-ADC7-7F7AC6F29286}'
    select @90max_resolver_clsid       = '{D604B4B5-686B-4304-9613-C4F82B527B10}'
    select @90download_resolver_clsid  = '{B57463F2-6ACE-4206-B600-CAB83A0847D2}'
    select @90mergetxt_resolver_clsid  = '{B451ED26-AB0C-4230-B989-F2959ECDDF22}'
    select @90subwins_resolver_clsid   = '{FF0EC373-ABAD-44dd-9ECC-9FB6BB55F54E}'
    select @90upload_resolver_clsid    = '{D6A60F76-F121-40e7-88A8-605BE6CE1EB5}'
    select @90sp_resolver_clsid        = '{87EC4491-1B75-4844-B7CF-090A1FB84BA6}'

    select @80additive_resolver_clsid   = '{08B0B2DB-3FB3-11D3-A4DE-00C04F610189}'
    select @80average_resolver_clsid    = '{08B0B2DC-3FB3-11D3-A4DE-00C04F610189}'
    select @80download_resolver_clsid   = '{08B0B2DD-3FB3-11D3-A4DE-00C04F610189}'
    select @80max_resolver_clsid        = '{08B0B2DE-3FB3-11D3-A4DE-00C04F610189}'
    select @80mergetxt_resolver_clsid   = '{08B0B2E1-3FB3-11D3-A4DE-00C04F610189}'
    select @80min_resolver_clsid        = '{08B0B2DF-3FB3-11D3-A4DE-00C04F610189}'
    select @80subwins_resolver_clsid    = '{08B0B2E0-3FB3-11D3-A4DE-00C04F610189}'
    select @80upload_resolver_clsid     = '{08B0B2E2-3FB3-11D3-A4DE-00C04F610189}'
    select @80sp_resolver_clsid         = '{08B0B2D6-3FB3-11D3-A4DE-00C04F610189}'
    

	-- Security check: User needs to be in at least one PAL
	if not exists (select * from dbo.sysmergepublications where 1 = {fn ISPALUSER(pubid)})
	begin
		return 1
	end
	

    if @resolver_clsid IS NULL
        return @resolver_clsid

    if @article_resolver IS NULL
        return NULL

    /* 
    ** We do not have new compatibility_level introduced from feature point of view in Denali. But
    ** for the com resolver class id, we need to do the conversion as well because side by side
    ** installation between Denali and previous version requires new class id used. Without this
    ** conversion the subscriber will get wrong class id and fail to register.
    */
    if @compatibility_level >= 17000000
        return @resolver_clsid

    if @compatibility_level >= 16000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @160additive_resolver_clsid
            when @average_resolver_clsid then @160average_resolver_clsid
            when @download_resolver_clsid then @160download_resolver_clsid
            when @max_resolver_clsid then @160max_resolver_clsid
            when @mergetxt_resolver_clsid then @160mergetxt_resolver_clsid
            when @min_resolver_clsid then @160min_resolver_clsid
            when @subwins_resolver_clsid then @160subwins_resolver_clsid
            when @upload_resolver_clsid then @160upload_resolver_clsid
            when @sp_resolver_clsid then @160sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 15000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @150additive_resolver_clsid
            when @average_resolver_clsid then @150average_resolver_clsid
            when @download_resolver_clsid then @150download_resolver_clsid
            when @max_resolver_clsid then @150max_resolver_clsid
            when @mergetxt_resolver_clsid then @150mergetxt_resolver_clsid
            when @min_resolver_clsid then @150min_resolver_clsid
            when @subwins_resolver_clsid then @150subwins_resolver_clsid
            when @upload_resolver_clsid then @150upload_resolver_clsid
            when @sp_resolver_clsid then @150sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 14000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @130additive_resolver_clsid
            when @average_resolver_clsid then @130average_resolver_clsid
            when @download_resolver_clsid then @130download_resolver_clsid
            when @max_resolver_clsid then @130max_resolver_clsid
            when @mergetxt_resolver_clsid then @130mergetxt_resolver_clsid
            when @min_resolver_clsid then @130min_resolver_clsid
            when @subwins_resolver_clsid then @130subwins_resolver_clsid
            when @upload_resolver_clsid then @130upload_resolver_clsid
            when @sp_resolver_clsid then @130sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 13000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @130additive_resolver_clsid
            when @average_resolver_clsid then @130average_resolver_clsid
            when @download_resolver_clsid then @130download_resolver_clsid
            when @max_resolver_clsid then @130max_resolver_clsid
            when @mergetxt_resolver_clsid then @130mergetxt_resolver_clsid
            when @min_resolver_clsid then @130min_resolver_clsid
            when @subwins_resolver_clsid then @130subwins_resolver_clsid
            when @upload_resolver_clsid then @130upload_resolver_clsid
            when @sp_resolver_clsid then @130sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 12000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @120additive_resolver_clsid
            when @average_resolver_clsid then @120average_resolver_clsid
            when @download_resolver_clsid then @120download_resolver_clsid
            when @max_resolver_clsid then @120max_resolver_clsid
            when @mergetxt_resolver_clsid then @120mergetxt_resolver_clsid
            when @min_resolver_clsid then @120min_resolver_clsid
            when @subwins_resolver_clsid then @120subwins_resolver_clsid
            when @upload_resolver_clsid then @120upload_resolver_clsid
            when @sp_resolver_clsid then @120sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 11000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @110additive_resolver_clsid
            when @average_resolver_clsid then @110average_resolver_clsid
            when @download_resolver_clsid then @110download_resolver_clsid
            when @max_resolver_clsid then @110max_resolver_clsid
            when @mergetxt_resolver_clsid then @110mergetxt_resolver_clsid
            when @min_resolver_clsid then @110min_resolver_clsid
            when @subwins_resolver_clsid then @110subwins_resolver_clsid
            when @upload_resolver_clsid then @110upload_resolver_clsid
            when @sp_resolver_clsid then @110sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 10000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @100additive_resolver_clsid
            when @average_resolver_clsid then @100average_resolver_clsid
            when @download_resolver_clsid then @100download_resolver_clsid
            when @max_resolver_clsid then @100max_resolver_clsid
            when @mergetxt_resolver_clsid then @100mergetxt_resolver_clsid
            when @min_resolver_clsid then @100min_resolver_clsid
            when @subwins_resolver_clsid then @100subwins_resolver_clsid
            when @upload_resolver_clsid then @100upload_resolver_clsid
            when @sp_resolver_clsid then @100sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 9000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @90additive_resolver_clsid
            when @average_resolver_clsid then @90average_resolver_clsid
            when @download_resolver_clsid then @90download_resolver_clsid
            when @max_resolver_clsid then @90max_resolver_clsid
            when @mergetxt_resolver_clsid then @90mergetxt_resolver_clsid
            when @min_resolver_clsid then @90min_resolver_clsid
            when @subwins_resolver_clsid then @90subwins_resolver_clsid
            when @upload_resolver_clsid then @90upload_resolver_clsid
            when @sp_resolver_clsid then @90sp_resolver_clsid
            -- new resolvers that exist in Yukon but not yet in Shiloh
            else @resolver_clsid 
        end
        return @resolver_clsid
    
    end
        
    -- 8.0 merge agent needs to map CLSIDs from all resolvers it has in common with 9.0
    if @compatibility_level >= 8000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @80additive_resolver_clsid
            when @average_resolver_clsid then @80average_resolver_clsid
            when @download_resolver_clsid then @80download_resolver_clsid
            when @max_resolver_clsid then @80max_resolver_clsid
            when @mergetxt_resolver_clsid then @80mergetxt_resolver_clsid
            when @min_resolver_clsid then @80min_resolver_clsid
            when @subwins_resolver_clsid then @80subwins_resolver_clsid
            when @upload_resolver_clsid then @80upload_resolver_clsid
            when @sp_resolver_clsid then @80sp_resolver_clsid
            -- new resolvers that exist in Yukon but not yet in Shiloh
            else @resolver_clsid 
        end
        return @resolver_clsid
    end
    
    -- need to map 7.0 clsid as well. only stored proc resolver shipped for 7.0
    if @compatibility_level >= 7000000 and @article_resolver = 'Microsoft SQLServer Stored Procedure Resolver'
        set @resolver_clsid = '{6F31CE30-7BE4-11d1-9B0A-00C04FC2DEB3}'
        
    return @resolver_clsid
    
    end


/*====  SQL Server 2022 version  ====*/

create function sys.fn_MSrepl_map_resolver_clsid (
    @compatibility_level  int,                      /* use 70 as the default compatibility level */
    @article_resolver  nvarchar(255),               /* article resolver name for verification purposes */
    @resolver_clsid  nvarchar(60)                   /* resolver class ID to be mapped */
    ) returns nvarchar(60)
    AS
    begin

    declare @additive_resolver_clsid    nvarchar(60)
    declare @average_resolver_clsid     nvarchar(60)
    declare @download_resolver_clsid    nvarchar(60)
    declare @max_resolver_clsid         nvarchar(60)
    declare @mergetxt_resolver_clsid    nvarchar(60)
    declare @min_resolver_clsid         nvarchar(60)
    declare @subwins_resolver_clsid     nvarchar(60)
    declare @upload_resolver_clsid      nvarchar(60)
    declare @sp_resolver_clsid          nvarchar(60)

    declare @150additive_resolver_clsid nvarchar(60)
    declare @150average_resolver_clsid  nvarchar(60)
    declare @150download_resolver_clsid nvarchar(60)
    declare @150max_resolver_clsid      nvarchar(60)
    declare @150mergetxt_resolver_clsid nvarchar(60)
    declare @150min_resolver_clsid      nvarchar(60)
    declare @150subwins_resolver_clsid  nvarchar(60)
    declare @150upload_resolver_clsid   nvarchar(60)
    declare @150sp_resolver_clsid       nvarchar(60)

	declare @140additive_resolver_clsid nvarchar(60)
    declare @140average_resolver_clsid  nvarchar(60)
    declare @140download_resolver_clsid nvarchar(60)
    declare @140max_resolver_clsid      nvarchar(60)
    declare @140mergetxt_resolver_clsid nvarchar(60)
    declare @140min_resolver_clsid      nvarchar(60)
    declare @140subwins_resolver_clsid  nvarchar(60)
    declare @140upload_resolver_clsid   nvarchar(60)
    declare @140sp_resolver_clsid       nvarchar(60)

    declare @130additive_resolver_clsid nvarchar(60)
    declare @130average_resolver_clsid  nvarchar(60)
    declare @130download_resolver_clsid nvarchar(60)
    declare @130max_resolver_clsid      nvarchar(60)
    declare @130mergetxt_resolver_clsid nvarchar(60)
    declare @130min_resolver_clsid      nvarchar(60)
    declare @130subwins_resolver_clsid  nvarchar(60)
    declare @130upload_resolver_clsid   nvarchar(60)
    declare @130sp_resolver_clsid       nvarchar(60)

    declare @120additive_resolver_clsid nvarchar(60)
    declare @120average_resolver_clsid  nvarchar(60)
    declare @120download_resolver_clsid nvarchar(60)
    declare @120max_resolver_clsid      nvarchar(60)
    declare @120mergetxt_resolver_clsid nvarchar(60)
    declare @120min_resolver_clsid      nvarchar(60)
    declare @120subwins_resolver_clsid  nvarchar(60)
    declare @120upload_resolver_clsid   nvarchar(60)
    declare @120sp_resolver_clsid       nvarchar(60)

    declare @110additive_resolver_clsid nvarchar(60)
    declare @110average_resolver_clsid  nvarchar(60)
    declare @110download_resolver_clsid nvarchar(60)
    declare @110max_resolver_clsid      nvarchar(60)
    declare @110mergetxt_resolver_clsid nvarchar(60)
    declare @110min_resolver_clsid      nvarchar(60)
    declare @110subwins_resolver_clsid  nvarchar(60)
    declare @110upload_resolver_clsid   nvarchar(60)
    declare @110sp_resolver_clsid       nvarchar(60)

    declare @100additive_resolver_clsid nvarchar(60)
    declare @100average_resolver_clsid  nvarchar(60)
    declare @100download_resolver_clsid nvarchar(60)
    declare @100max_resolver_clsid      nvarchar(60)
    declare @100mergetxt_resolver_clsid nvarchar(60)
    declare @100min_resolver_clsid      nvarchar(60)
    declare @100subwins_resolver_clsid  nvarchar(60)
    declare @100upload_resolver_clsid   nvarchar(60)
    declare @100sp_resolver_clsid       nvarchar(60)

    declare @90additive_resolver_clsid  nvarchar(60)
    declare @90average_resolver_clsid   nvarchar(60)
    declare @90download_resolver_clsid  nvarchar(60)
    declare @90max_resolver_clsid       nvarchar(60)
    declare @90mergetxt_resolver_clsid  nvarchar(60)
    declare @90min_resolver_clsid       nvarchar(60)
    declare @90subwins_resolver_clsid   nvarchar(60)
    declare @90upload_resolver_clsid    nvarchar(60)
    declare @90sp_resolver_clsid        nvarchar(60)

    declare @80additive_resolver_clsid  nvarchar(60)
    declare @80average_resolver_clsid   nvarchar(60)
    declare @80download_resolver_clsid  nvarchar(60)
    declare @80max_resolver_clsid       nvarchar(60)
    declare @80mergetxt_resolver_clsid  nvarchar(60)
    declare @80min_resolver_clsid       nvarchar(60)
    declare @80subwins_resolver_clsid   nvarchar(60)
    declare @80upload_resolver_clsid    nvarchar(60)
    declare @80sp_resolver_clsid        nvarchar(60)

    select @additive_resolver_clsid  = '{6ACA9C22-3CC2-4947-9D5F-525A1F9E8B45}'
    select @average_resolver_clsid   = '{F09F3613-7C9A-481F-952D-84B5E1060AC6}'
    select @download_resolver_clsid  = '{B4CA61C8-7495-4F5A-9EE1-AEAF685693C8}'
    select @max_resolver_clsid       = '{608EB36F-373A-4485-A1AA-A21DE806FDF1}'
    select @mergetxt_resolver_clsid  = '{C6CE3676-53F4-47B5-B3AF-1BB44996192E}'
    select @min_resolver_clsid       = '{CCC9BC97-EC98-4210-9BA0-2FE28C6DE077}'
    select @subwins_resolver_clsid   = '{BB6E90FC-FFE5-4256-9906-56BDFD7F1CAB}'
    select @upload_resolver_clsid    = '{482BCCD2-3FEB-4CA3-84C6-A380E3AB10E8}'
    select @sp_resolver_clsid        = '{D2701A2D-9D79-41BC-B7C4-1F2B5CF891B7}'

    select @150additive_resolver_clsid  = '{D8A56ACA-5796-4701-8C06-87CC3B8A3BFD}'
    select @150average_resolver_clsid   = '{B1A99B7D-0CA5-4C47-B18A-EEF10D6D7B54}'
    select @150download_resolver_clsid  = '{9A14C179-42AE-4F99-B2EB-078F9892991E}'
    select @150max_resolver_clsid       = '{10D3B90F-EC51-495F-BC3F-74BFAB3850E9}'
    select @150mergetxt_resolver_clsid  = '{EFCA64BD-6289-4F30-8517-414190E2B5C8}'
    select @150min_resolver_clsid       = '{8D47A9E4-5EAE-40A1-90C7-CC099C165795}'
    select @150subwins_resolver_clsid   = '{EA19DB37-5534-4D93-95EF-70BA004DD29B}'
    select @150upload_resolver_clsid    = '{01F9206D-C615-4E1D-8C06-90A7A2A3F3A9}'
    select @150sp_resolver_clsid        = '{048131BB-09DD-4B6E-BF67-9B7347AD710F}'

	select @140additive_resolver_clsid = '{BFC711F4-5750-4CB3-B7FB-FDE31FC54DB3}'
    select @140average_resolver_clsid  = '{D466CB4D-A131-4067-984F-F1416BBECD5A}'
    select @140download_resolver_clsid = '{087ABE5E-9ABC-43F3-AD45-D38F0BBEA31D}'
    select @140max_resolver_clsid      = '{63114856-E965-4FAB-9D90-5756795FA484}'
    select @140mergetxt_resolver_clsid = '{C61365C1-437A-460C-92C7-3F06CAD44F22}'
    select @140min_resolver_clsid      = '{E76EED9D-B1F5-4E10-9CD4-7D7A63FAEDB7}'
    select @140subwins_resolver_clsid  = '{D45D6C0D-E6B2-4E0E-91C6-2E64A49757F2}'
    select @140upload_resolver_clsid   = '{F550DC00-B806-497A-99EC-636480D560D8}'
    select @140sp_resolver_clsid       = '{173648B0-37B3-4A3B-9BD3-8627E65C6EE5}'

    select @130additive_resolver_clsid = '{D2691D5C-F78E-4566-A818-B2A1092ACBEE}'
    select @130average_resolver_clsid  = '{C85D9955-8790-4DE6-9EB9-BE6A3FE72025}'
    select @130download_resolver_clsid = '{C688A401-46DD-4A4E-8B8E-C293A3E4D3FA}'
    select @130max_resolver_clsid      = '{9FEABCBE-50F2-4DC5-B323-E9E9A81B564E}'
    select @130mergetxt_resolver_clsid = '{F19522DD-0627-4AE3-8B8F-3C4363B3D4C8}'
    select @130min_resolver_clsid      = '{6EF1F47D-880B-42B3-A939-A5884202F3DE}'
    select @130subwins_resolver_clsid  = '{56604C05-9197-4D50-AF78-F45E201E495D}'
    select @130upload_resolver_clsid   = '{C0FCE632-D230-49FC-ACA0-968F20CD6D40}'
    select @130sp_resolver_clsid       = '{163EBC43-A5CA-4972-960A-0F2B1CE8D6EF}'

    select @120additive_resolver_clsid = '{408547ED-AA55-4077-AD6E-621B279DC81C}'
    select @120average_resolver_clsid  = '{0BEE7B25-09E9-4B74-AA8C-B306EA1C4072}'
    select @120download_resolver_clsid = '{66BAF4FC-ED0F-4479-9397-7FBCCFB3FAF2}'
    select @120max_resolver_clsid      = '{51F9F46D-7497-42F7-8F4D-E4B1724598AE}'
    select @120mergetxt_resolver_clsid = '{FE6E5C13-A27E-4BFD-A0B7-5B60017D20BF}'
    select @120min_resolver_clsid      = '{95EDC0AD-AA41-43F6-8F44-400633DFEF2E}'
    select @120subwins_resolver_clsid  = '{DBD898EC-A745-4992-BCAA-485161E736A9}'
    select @120upload_resolver_clsid   = '{33AFDE81-1461-41CF-9930-38E5AFFFC379}'
    select @120sp_resolver_clsid       = '{64C5CD68-54A3-4961-8053-445321C795E2}'
    
    select @110additive_resolver_clsid = '{8FE7FF34-7C5D-4BE7-8056-ADB6D6F692DC}'
    select @110average_resolver_clsid  = '{376F678E-4691-43E8-8AE7-DAD8CAA644EF}'
    select @110download_resolver_clsid = '{3BB9F418-3407-4F5B-8DB3-9E9147C3A710}'
    select @110max_resolver_clsid      = '{7365BF95-62E8-4B72-A0F7-E238FE413DB7}'
    select @110mergetxt_resolver_clsid = '{9DCD5250-86BB-433D-8C1F-561460105CF0}'
    select @110min_resolver_clsid      = '{93277AB4-C338-48B8-9A4A-CA5A32587AB7}'
    select @110subwins_resolver_clsid  = '{77E52C5E-0016-4EDF-9391-8C07BFB668CE}'
    select @110upload_resolver_clsid   = '{3D43EBE7-063C-4447-91E7-DE7A264C8441}'
    select @110sp_resolver_clsid       = '{3BB074FA-0836-4A63-BE0C-AF49DDD42A1C}'

    select @100additive_resolver_clsid = '{D2CCB059-65DD-497B-8822-7660B7778DDF}'
    select @100average_resolver_clsid  = '{91DD61BF-D937-4A21-B0EF-36204A328439}'
    select @100download_resolver_clsid = '{9602B431-2937-4D51-8CC3-11F8AC1EC26D}'
    select @100max_resolver_clsid      = '{77209412-47CF-49AF-A347-DCF7EE481277}'
    select @100mergetxt_resolver_clsid = '{0045200C-9126-4432-BC9B-3186D141EB5A}'
    select @100min_resolver_clsid      = '{2FF7564F-9D55-48C0-A4C1-C148076D9119}'
    select @100subwins_resolver_clsid  = '{E93406CC-5879-4143-B70B-29B385BA80C9}'
    select @100upload_resolver_clsid   = '{05614E0C-92A9-45F3-84A4-46C8E36424A9}'
    select @100sp_resolver_clsid       = '{D264B5C0-1300-471A-80C9-9C1FC34A3691}'

    select @90additive_resolver_clsid  = '{4B385BCE-190B-46c5-AEAB-51358A8E1CB6}'
    select @90average_resolver_clsid   = '{337754AA-CF6E-4be8-8F13-F6AC91524EDC}'
    select @90min_resolver_clsid       = '{464CBD74-3177-4593-ADC7-7F7AC6F29286}'
    select @90max_resolver_clsid       = '{D604B4B5-686B-4304-9613-C4F82B527B10}'
    select @90download_resolver_clsid  = '{B57463F2-6ACE-4206-B600-CAB83A0847D2}'
    select @90mergetxt_resolver_clsid  = '{B451ED26-AB0C-4230-B989-F2959ECDDF22}'
    select @90subwins_resolver_clsid   = '{FF0EC373-ABAD-44dd-9ECC-9FB6BB55F54E}'
    select @90upload_resolver_clsid    = '{D6A60F76-F121-40e7-88A8-605BE6CE1EB5}'
    select @90sp_resolver_clsid        = '{87EC4491-1B75-4844-B7CF-090A1FB84BA6}'

    select @80additive_resolver_clsid   = '{08B0B2DB-3FB3-11D3-A4DE-00C04F610189}'
    select @80average_resolver_clsid    = '{08B0B2DC-3FB3-11D3-A4DE-00C04F610189}'
    select @80download_resolver_clsid   = '{08B0B2DD-3FB3-11D3-A4DE-00C04F610189}'
    select @80max_resolver_clsid        = '{08B0B2DE-3FB3-11D3-A4DE-00C04F610189}'
    select @80mergetxt_resolver_clsid   = '{08B0B2E1-3FB3-11D3-A4DE-00C04F610189}'
    select @80min_resolver_clsid        = '{08B0B2DF-3FB3-11D3-A4DE-00C04F610189}'
    select @80subwins_resolver_clsid    = '{08B0B2E0-3FB3-11D3-A4DE-00C04F610189}'
    select @80upload_resolver_clsid     = '{08B0B2E2-3FB3-11D3-A4DE-00C04F610189}'
    select @80sp_resolver_clsid         = '{08B0B2D6-3FB3-11D3-A4DE-00C04F610189}'
    

	-- Security check: User needs to be in at least one PAL
	if not exists (select * from dbo.sysmergepublications where 1 = {fn ISPALUSER(pubid)})
	begin
		return 1
	end
	

    if @resolver_clsid IS NULL
        return @resolver_clsid

    if @article_resolver IS NULL
        return NULL

    /* 
    ** We do not have new compatibility_level introduced from feature point of view in Denali. But
    ** for the com resolver class id, we need to do the conversion as well because side by side
    ** installation between Denali and previous version requires new class id used. Without this
    ** conversion the subscriber will get wrong class id and fail to register.
    */
    if @compatibility_level >= 16000000
        return @resolver_clsid
		
    if @compatibility_level >= 15000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @150additive_resolver_clsid
            when @average_resolver_clsid then @150average_resolver_clsid
            when @download_resolver_clsid then @150download_resolver_clsid
            when @max_resolver_clsid then @150max_resolver_clsid
            when @mergetxt_resolver_clsid then @150mergetxt_resolver_clsid
            when @min_resolver_clsid then @150min_resolver_clsid
            when @subwins_resolver_clsid then @150subwins_resolver_clsid
            when @upload_resolver_clsid then @150upload_resolver_clsid
            when @sp_resolver_clsid then @150sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 14000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @130additive_resolver_clsid
            when @average_resolver_clsid then @130average_resolver_clsid
            when @download_resolver_clsid then @130download_resolver_clsid
            when @max_resolver_clsid then @130max_resolver_clsid
            when @mergetxt_resolver_clsid then @130mergetxt_resolver_clsid
            when @min_resolver_clsid then @130min_resolver_clsid
            when @subwins_resolver_clsid then @130subwins_resolver_clsid
            when @upload_resolver_clsid then @130upload_resolver_clsid
            when @sp_resolver_clsid then @130sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 13000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @130additive_resolver_clsid
            when @average_resolver_clsid then @130average_resolver_clsid
            when @download_resolver_clsid then @130download_resolver_clsid
            when @max_resolver_clsid then @130max_resolver_clsid
            when @mergetxt_resolver_clsid then @130mergetxt_resolver_clsid
            when @min_resolver_clsid then @130min_resolver_clsid
            when @subwins_resolver_clsid then @130subwins_resolver_clsid
            when @upload_resolver_clsid then @130upload_resolver_clsid
            when @sp_resolver_clsid then @130sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 12000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @120additive_resolver_clsid
            when @average_resolver_clsid then @120average_resolver_clsid
            when @download_resolver_clsid then @120download_resolver_clsid
            when @max_resolver_clsid then @120max_resolver_clsid
            when @mergetxt_resolver_clsid then @120mergetxt_resolver_clsid
            when @min_resolver_clsid then @120min_resolver_clsid
            when @subwins_resolver_clsid then @120subwins_resolver_clsid
            when @upload_resolver_clsid then @120upload_resolver_clsid
            when @sp_resolver_clsid then @120sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 11000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @110additive_resolver_clsid
            when @average_resolver_clsid then @110average_resolver_clsid
            when @download_resolver_clsid then @110download_resolver_clsid
            when @max_resolver_clsid then @110max_resolver_clsid
            when @mergetxt_resolver_clsid then @110mergetxt_resolver_clsid
            when @min_resolver_clsid then @110min_resolver_clsid
            when @subwins_resolver_clsid then @110subwins_resolver_clsid
            when @upload_resolver_clsid then @110upload_resolver_clsid
            when @sp_resolver_clsid then @110sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 10000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @100additive_resolver_clsid
            when @average_resolver_clsid then @100average_resolver_clsid
            when @download_resolver_clsid then @100download_resolver_clsid
            when @max_resolver_clsid then @100max_resolver_clsid
            when @mergetxt_resolver_clsid then @100mergetxt_resolver_clsid
            when @min_resolver_clsid then @100min_resolver_clsid
            when @subwins_resolver_clsid then @100subwins_resolver_clsid
            when @upload_resolver_clsid then @100upload_resolver_clsid
            when @sp_resolver_clsid then @100sp_resolver_clsid
            else @resolver_clsid 
        end
        return @resolver_clsid
    end

    if @compatibility_level >= 9000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @90additive_resolver_clsid
            when @average_resolver_clsid then @90average_resolver_clsid
            when @download_resolver_clsid then @90download_resolver_clsid
            when @max_resolver_clsid then @90max_resolver_clsid
            when @mergetxt_resolver_clsid then @90mergetxt_resolver_clsid
            when @min_resolver_clsid then @90min_resolver_clsid
            when @subwins_resolver_clsid then @90subwins_resolver_clsid
            when @upload_resolver_clsid then @90upload_resolver_clsid
            when @sp_resolver_clsid then @90sp_resolver_clsid
            -- new resolvers that exist in Yukon but not yet in Shiloh
            else @resolver_clsid 
        end
        return @resolver_clsid
    
    end
        
    -- 8.0 merge agent needs to map CLSIDs from all resolvers it has in common with 9.0
    if @compatibility_level >= 8000000
    begin
        select @resolver_clsid = 
        case @resolver_clsid
            when @additive_resolver_clsid then @80additive_resolver_clsid
            when @average_resolver_clsid then @80average_resolver_clsid
            when @download_resolver_clsid then @80download_resolver_clsid
            when @max_resolver_clsid then @80max_resolver_clsid
            when @mergetxt_resolver_clsid then @80mergetxt_resolver_clsid
            when @min_resolver_clsid then @80min_resolver_clsid
            when @subwins_resolver_clsid then @80subwins_resolver_clsid
            when @upload_resolver_clsid then @80upload_resolver_clsid
            when @sp_resolver_clsid then @80sp_resolver_clsid
            -- new resolvers that exist in Yukon but not yet in Shiloh
            else @resolver_clsid 
        end
        return @resolver_clsid
    end
    
    -- need to map 7.0 clsid as well. only stored proc resolver shipped for 7.0
    if @compatibility_level >= 7000000 and @article_resolver = 'Microsoft SQLServer Stored Procedure Resolver'
        set @resolver_clsid = '{6F31CE30-7BE4-11d1-9B0A-00C04FC2DEB3}'
        
    return @resolver_clsid
    
    end

