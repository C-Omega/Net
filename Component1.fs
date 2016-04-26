namespace C_Omega
module Extensions = 
    module Seq =
        let takeFirst<'a> f (s:seq<'a>) = 
            let mutable v = None
            v,seq{for i in s do if f i then v <- Some i else yield i}
    module Array = 
        let takeFirst<'a> f (s:'a[]) = 
            let mutable v = None
            v,[|for i in s do if f i then v <- Some i else yield i|]
    module List =
        let takeFirst<'a> f (s:'a list) = 
            let mutable v = None
            v,[for i in s do if f i then v <- Some i else yield i]
open Extensions
module Helpers = 
    ///A exception for breaking a loop
    exception ValueFoundException of obj
    type Logger = 
        {log : string -> unit}
        static member Printn = {log = System.Console.WriteLine}
    type Stopwatch() = 
        let s = new System.Diagnostics.Stopwatch()
        member x.Start() = s.Start()
        member x.Stop() = s.Stop()
        member x.Elapsed = s.ElapsedMilliseconds
        member x.Reset() = s.Reset()
        member x.HasStarted = s.IsRunning
    let justreturn v = fun _ -> v
    let enum<'a,'b when 'a : enum<'b>> : 'b->'a = LanguagePrimitives.EnumOfValue
    let deEnum<'a,'b when 'a : enum<'b>> : 'a->'b = LanguagePrimitives.EnumToValue
    let _b_uint16 b = System.BitConverter.ToUInt16(b,0)
    let _b_uint64 b = System.BitConverter.ToUInt64(b,0)
    let _uint64_b (i:uint64) = System.BitConverter.GetBytes i
    let _uint16_b (i:uint16) = System.BitConverter.GetBytes i
    let _uint32_b (i:uint32) = System.BitConverter.GetBytes i
    let _b_uint32 b = System.BitConverter.ToUInt32(b,0)
    let (|Empty|_|) s = if Seq.isEmpty s then Some(Empty) else None
    let (|IsIn|_|) s e = if Seq.contains e s then Some IsIn else None
    let (|IsNotIn|_|) s e = if Seq.contains e s then None else Some IsNotIn
    let (|F|_|) f v = if f v then Some(F) else None
    let r = System.Random()
    let randb n = let b = Array.zeroCreate n in r.NextBytes b;b
    let rand_int64() = r.NextDouble()|>System.BitConverter.DoubleToInt64Bits
    let _int64_b (i:int64) = System.BitConverter.GetBytes(i)
    let _b_int64 b = System.BitConverter.ToInt64(b,0)
    let _int32_b (i:int32) = System.BitConverter.GetBytes(i)
    let _b_int32 b = System.BitConverter.ToInt32(b,0)
    let loopbackv4 = System.Net.IPAddress.Loopback
    let loopbackv6 = System.Net.IPAddress.IPv6Loopback
    let ip = System.Net.IPAddress.Parse
    let anyv4 = System.Net.IPAddress.Any
    let anyv6 = System.Net.IPAddress.IPv6Any
    let ep (a:System.Net.IPAddress) p = System.Net.IPEndPoint(a,p)
    let flipc f = fun b a -> f a b
    let flipt f = function |b,a -> a,b
    let both a b v = (a v,b v)
    let side a v = a v; v
    let spinuntil f = System.Threading.SpinWait.SpinUntil(System.Func<_> f)
    let spin i = System.Threading.SpinWait.SpinUntil(System.Func<_>(fun _ -> false),(i:int))|>ignore
    let spinuntilreturn f = 
        let v = ref None
        System.Threading.SpinWait.SpinUntil(System.Func<_>(fun() -> match f() with |None -> false |v' -> v := v';true))
        v.Value.Value
    let timeoutyield f t =
        let v = ref None
        System.Threading.SpinWait.SpinUntil(System.Func<_>(fun() -> match f() with |None -> false |v' -> v := v';true),(t:int))|>ignore
        v.Value
    let shuffle(v:'a[]) = 
        let v = Array.copy v
        let f() = r.Next(0,v.Length)
        let swap a b = 
            let c = v.[a]
            v.[a] <- v.[b]
            v.[b] <- c
        for i = 0 to v.Length - 1 do f()|> swap i
        v
    let arg (f:'a->'b->'c) (v:'a) = f v
    let xor = Array.map2 (arg (^^^))
type Mapper<'a,'b> =  
    {map:'a->'b;unmap:'b->'a}
    static member Delay i = {map = (fun a -> Helpers.spin i; a); unmap = fun a -> Helpers.spin i;a}
type Verifier<'a,'b> = {sign : 'a -> 'b;verify : 'b -> 'a option}
type ev<'a> = Verifier<'a,'a>
type bv = ev<byte[]>
module Comm = 
    open System.Net
    open Helpers
    exception WrongCommType
    type SendReceive<'a> = 
        {send : 'a -> unit;receive : unit -> 'a}
        member x.Map (m:Mapper<'b,_>) = {send = m.map >> x.send;receive = x.receive >> m.unmap}
        member x.Log (logger:Logger) = {send = x.send >> side (logger.log << (sprintf ">%A")); receive = x.receive >> side (logger.log << (sprintf "<%A"))}
        member x.FlushTo (v:'a) = if Unchecked.equals v (x.receive()) then () else x.FlushTo v
    type bsr = SendReceive<byte[]>
    type BufferedSendReceive<'a>(sr:SendReceive<'a>) = 
        let v = new ResizeArray<'a>()
        let s = ref true
        do async{while !s do let x = sr.receive() in lock v (fun() -> v.Add x)}|>Async.Start
        interface System.IDisposable with member x.Dispose() = s:=false
        member x.TryTake(f:'a->bool) = lock v (fun() ->
            match v.FindIndex(System.Predicate f) with
            | -1 -> None
            |i ->
                let j = v.[i]
                v.RemoveAt i
                Some j
            )
        member x.Contains i = v.Contains i
        member x.Send v = sr.send v
        member x.Exists f = lock v (fun() -> v.Exists(System.Predicate f))
        member x.Take f = spinuntil(fun () -> x.Exists f);x.TryTake f|>Option.get
        //member x.Stop() = (x:>System.IDisposable).Dispose()
        member x.Start() = if not !s then async{while !s do sr.receive()|>v.Add}|>Async.Start
        member x.Receive() = lock v (fun() -> let j = v.[0] in v.RemoveAt 0; j)
        member x.SendRecieve() = {send = x.Send; receive = x.Receive}
        member x.Count = v.Count
        member x.Buffer = v
        member x.TryTakeMap (f:'a->'b option) = lock v (fun() ->
            try
                for i,j in Seq.indexed v do
                    match f j with 
                    |None -> ()
                    |k -> 
                        v.RemoveAt i
                        raise(ValueFoundException(k))
                None
            with |ValueFoundException(v) -> v|>unbox<'b option>
            )
        member x.TakeMap f = Helpers.spinuntilreturn (fun () -> x.TryTakeMap f)
    type bbsr = BufferedSendReceive<byte[]>
    let udpIP (l:IPEndPoint) (r:IPEndPoint) : bsr = 
        let s = new Sockets.Socket(l.AddressFamily,Sockets.SocketType.Dgram,Sockets.ProtocolType.Udp)
        s.Bind l
        s.Connect r
        {
            send = s.Send >> ignore//fun b -> s.SendTo(b,r)|>ignore
            receive = fun () -> let b = Array.zeroCreate<byte> 65536 in b.[..s.Receive(b)-1]
        }
    type PushPull<'a,'b> = {push:'a->'b; pull:'b->'a}
    type Comm<'a,'b> = |PushPull of PushPull<'a,'b> |SendReceive of SendReceive<'a>
    [<System.FlagsAttribute>]
    type CommType = |PushPull = 1 |SendRecieve = 2
    type bc = Comm<byte[],uint16>
module Net =
    open Helpers
    open Comm
    type Connection<'a> = {sr:SendReceive<'a>;disconnect:unit->unit}
    type Connector<'a,'b> = {connect:'a -> Connection<'b>;close : unit -> unit}
    type SCConnector<'a,'c,'b> = {server:'a -> Connection<'b>;client:'c->Connection<'b>;close:unit->unit}
    type bcon = Connection<byte[]>
    type bcor<'a> = Connector<'a,byte[]>
    type bccor = Connector<bc,byte[]>
    type bscc<'a,'b> = SCConnector<'a,'b,byte[]>
    type bcscc = SCConnector<bc,bc,byte[]>
    type escc<'a,'b> = SCConnector<'a,'a,'b>
    exception AddressParseException
    [<AllowNullLiteral>]
    ///<param name="bytes">Must be a multiple of 8 long (8,16,24,32)...</param>
    type Address(bytes:byte[]) = 
        override x.Equals o = (unbox<Address> o).ToString() = x.ToString()
        override x.GetHashCode() = x.ToString().GetHashCode()
        member x.Bytes = bytes
        member x.AsUInt64Array() = [for i in [0..8..bytes.Length-1] do yield bytes.[i..i+7]|>Array.rev |> _b_uint64 ]
        member x.Item i = Address(bytes.[i*8..((i+1)*8)-1])
        member x.Depth = bytes.Length/8|>uint16
        member x.GetSlice(a,b) = 
            let b = Option.map (fun (i:int) -> i*8) b
            Address(bytes.[(defaultArg a 0)*8 .. defaultArg b (bytes.Length) - 1])
        override x.ToString() = 
            [for i in x.AsUInt64Array() do 
                let n = System.String.Format("{0:X}",i).PadLeft(16,'0') 
                yield "("+n.Insert(4,"|").Insert(9,"|").Insert(14,"|")+")"
            ]
            |> String.concat "~"
        member x.KnockOne() = x.[0],x.[1..]
        static member (+) (a:Address,b:Address) = Address(Array.append a.Bytes b.Bytes)
        static member None = Address([|255uy;255uy;255uy;255uy;255uy;255uy;255uy;255uy;|])
        static member Central = Address(Array.zeroCreate 8)
        static member Handler = Address.Parse("(aaaa|aaaa|aaaa|aaaa)")
        static member Parse(s':string) = 
                Address(s'.Split('~')
                |> Array.map (fun (s:string) ->
                    let v = 
                        s.Replace("||",String.replicate((22-s.Length)/4) "0000")
                        |>String.filter(function|'('|'|'|')'->false|_->true)
                    if v.Length <> 16 then raise AddressParseException
                    Array.init 8 (fun i -> let i' = i*2 in System.Convert.ToByte(v.[i'..i'+1],16))
                    )
                |> Array.concat)
    module Headers = 
        exception NegotiationFailed
        type HeaderType = |INIT = 0uy|OPTIONS = 1uy|ALL = 2uy|FIXED = 3uy|KNOWN = 4uy|MULTI = 5uy|TUNNEL = 6uy|OTHER=255uy
        type init(b:HeaderType) = 
            member x.RequestedType = b
            member x.ToByteArray() = [|0uy;b|>deEnum|]
            static member OfByte b = enum<HeaderType,byte> b
        let negotiate (h:seq<HeaderType>) (sr:bc) (logger:Logger) = 
            let log = logger.log
            let r() = (match sr with |PushPull(p) -> p.pull 2us |SendReceive(s) -> s.receive())
            let s = (match sr with |PushPull(p) -> p.push >> ignore |SendReceive(s) -> s.send)
            let rec flush() = 
                match r() with 
                |[|0uy;0uy|] -> () 
                |_ -> flush()
            let rec inner ro h' =
                if Seq.isEmpty h' then 
                    log "No more new elements, sending empty."
                    s [|0uy;0uy|]
                    if ro then log "No common protocol. Going with ALL."; s [|0uy;2uy|];HeaderType.ALL else
                    let rec finish() = 
                        match r() with
                        |[|0uy;0uy|] -> 
                            log "Remote has run out of elements as well"
                            HeaderType.ALL
                        |[|0uy;a|] as p when Seq.contains (enum a) h -> 
                            s p
                            enum<HeaderType,byte> a
                        |[|0uy;c|] -> s [|0uy;1uy|];finish()
                        |_ -> s [|0uy;0uy|]; log "Non-INIT message"; raise NegotiationFailed
                    finish()
                else
                    let req' = Seq.head h'
                    sprintf "Requesting %A" req' |> log
                    let req = req' |> deEnum
                    s [|0uy;req|]
                    let rec go(runout) : HeaderType= 
                        match r() with
                        |[|0uy;0uy|] -> 
                            log "Remote has run out of elements"
                            go(true)
                        |[|0uy;v|] as p when v = req -> s [|0uy;0uy|]; (if not runout then log "Flushing"; flush()); enum<HeaderType,byte> req
                        |[|0uy;a|] as p when Seq.contains (enum a) h -> 
                            s p
                            s [|0uy;0uy|]
                            (if not runout then log "Flushing"; flush())
                            enum<HeaderType,byte> a
                        |[|0uy;v|] -> sprintf "Denied %A and suggested %A" req' (enum<HeaderType,_> v)|> log;Seq.tail h'|>inner runout
                        |_ -> s [|0uy;0uy|]; log "Non-INIT message"; raise NegotiationFailed
                    go ro
            let a = inner false h
            sprintf "Accepted %A" a |> log
            a
        type PacketType<'a> = {GetPacket : bc -> 'a;MakePacket : 'a -> byte[]; HeaderType : HeaderType;CommType : CommType;GetData:'a->byte[]}

        type _ALL = 
            {next:Address;data:byte[];footer:Address}
            static member PacketType = 
                {
                    GetPacket = (function 
                        |SendReceive(s) -> 
                            let h,(i,j)=s.receive()|>Array.splitAt 12|>function |a,b -> a,Array.splitAt (int (_b_uint16 a.[8..])) b
                            {next=Address(h.[..7]);data=i;footer=Address(j)}
                        |PushPull(p) -> 
                            let header = p.pull 12us
                            {next=Address(header.[..7]);data=header.[8..9]|>_b_uint16|>p.pull;footer=Address(header.[10..]|>_b_uint16|>p.pull)}
                        )
                    MakePacket = fun i -> 
                        let j = i.footer.Bytes 
                        Array.concat [|i.next.Bytes;i.data.Length|>uint16|>_uint16_b;j.Length|>uint16|>_uint16_b;i.data;j|]
                    HeaderType = HeaderType.ALL
                    CommType = CommType.PushPull|||CommType.SendRecieve
                    GetData = (function i -> i.data)
                }
        type _KNOWN = 
            {next:Address;data:byte[];footer:Address}
            static member PacketType = 
                {
                    GetPacket = (function
                        |SendReceive(s) -> 
                            let h,(i,j)=
                                let v = s.receive()
                                v
                                |>Array.splitAt 10
                                |>function |a,b -> a,Array.splitAt (int (_b_uint16 a.[8..])) b
                            {next=Address(h.[..7]);data=i;footer=Address(j)}
                        |PushPull(p) -> raise WrongCommType
                        )
                    MakePacket = (fun i -> Array.concat [|i.next.Bytes;i.data.Length|>uint16|>_uint16_b;i.data;i.footer.Bytes|])
                    HeaderType = HeaderType.KNOWN
                    CommType = CommType.SendRecieve
                    GetData = (function i -> i.data)
                }
        //let packettypes = Map<HeaderType,obj>([|HeaderType.ALL,box _ALL.PacketType;HeaderType.KNOWN,box _KNOWN.PacketType|])
        let ptbsr (pt:PacketType<'a>) m (comm:bc) : bsr = 
            let s = 
                match comm with 
                |SendReceive(s) -> s.send
                |PushPull(p) -> p.push>>ignore
            {send = m >> s; receive = justreturn <| pt.GetPacket comm>>pt.GetData}
    module Protocols = 
        let isoppannounce (b:byte[]) = b.Length>3&&b.[3]&&&128uy<>0uy 
        exception BadPackets
        let OPP size : Mapper<byte[],byte[][]> = {
            map = fun b -> 
                Array.chunkBySize size b
                |> Array.Parallel.mapi(fun i v -> Array.append (_int32_b i) v)|>
                Array.append [|Array.append (_int32_b (-size)) (_int32_b b.Length)|]
            unmap = fun b->
                let a,b = match Array.takeFirst<byte[]> isoppannounce b with |Some(v),b -> v,b |_ -> raise BadPackets
                let s,l = -(_b_int32 a),_b_int32 a.[4..]
                let b' = Array.zeroCreate l
                Array.Parallel.iter<byte[]> (function |b'' -> Array.blit b''.[4..] 0 b' ((_b_int32 a)*s) (b''.Length-4)) b
                b'
            }
        let OPPStream (announce : byte[]) (r:unit -> byte[]) = 
            let s,l = -(_b_int32 announce), _b_int32 announce.[4..]
            let b' = Array.zeroCreate l
            let left = ref(if l%s > 0 then l/s + 1 else l/s)
            async{
                let b = r()
                while !left > 0 do 
                    do! async{
                        let a,b'' = _b_int32 b, b.[4..]
                        Array.blit b'' 0 b' (a*s) (b''.Length)
                        System.Threading.Interlocked.Add(left,-1)|>ignore
                   }
            }|>Async.Start
            b',(fun () -> left := 0),left
        exception RPPTimeout
        let RPP timeout tryiter : Connector<bbsr,byte[]> = 
            {
                connect = fun x ->
                    let v = 
                        let b1 = randb 8
                        let e = Array.zeroCreate 8
                        x.Send(Array.append e b1)
                        x.Take(fun b -> b.[..7] = e)|>Array.skip 8 |> xor b1
                    let f (b:byte[]) = b.[..7] = v && b.Length > 16
                    {
                        sr = 
                            {
                                send = fun b -> 
                                    let n = randb 8 |> Array.append v
                                    if tryiter < 0 then 
                                        spinuntil(fun() -> 
                                            Array.concat [|n;b|] |> x.Send
                                            timeoutyield (fun() -> x.TryTake (arg (=) n)) timeout |> Option.isSome
                                        )
                                    else
                                        let rec go i =
                                            if i = 0 then raise RPPTimeout else
                                                Array.concat [|n;b|] |> x.Send
                                                if timeoutyield (fun() -> x.TryTake (arg (=) n)) timeout |> Option.isNone then go (i-1)
                                        go tryiter
                                receive = fun () -> x.Take f |> Array.splitAt 16 |> function |a,b -> x.Send a;b
                            }
                        disconnect = ignore//x.Stop
                    }
                close = ignore
            }
        let GPP(i:uint64) = 
            let b = _uint64_b i
            {sign = Array.append b;verify=Array.splitAt 8>>function |F(arg(=)b),v -> Some v |_-> None}
        let gropp size timeout tryiter : Connector<bbsr,byte[]> = 
            let s = RPP timeout tryiter
            {
                connect = fun x ->
                    let rpp = new bbsr((s.connect x).sr)
                    {
                        sr = 
                            {
                                send = (OPP size).map >> Array.Parallel.map(GPP(randb 8 |> _b_uint64).sign) >> Array.Parallel.iter rpp.Send
                                receive = fun() ->
                                    let g,v = rpp.Take (Array.skip 8 >> isoppannounce) |> Array.splitAt 8 |> function |a,b -> GPP(_b_uint64 a).verify,b
                                    let b,s,l = OPPStream v (fun() -> rpp.TakeMap g)
                                    spinuntil(fun() -> !l = 0)|>ignore
                                    b

                            }
                        disconnect = justreturn()
                    }
                close = ignore
            }

        let assemblingRPP timeout tryiter bbsr size =
            let rpp = RPP timeout tryiter
            let bbsr = rpp.connect(bbsr).sr
            {
                send = 
                    Array.chunkBySize size
                    >> fun b -> Array.append [|b.Length - 1 |> uint32 |> _uint32_b|] b
                    >> Array.iter bbsr.send
                receive = fun () -> 
                    let h = bbsr.receive()|>_b_uint32|>int
                    [|for i = 0 to h do yield bbsr.receive()|]|>Array.concat
            }