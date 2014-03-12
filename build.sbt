import ScalateKeys._

seq(scalateSettings:_*)

// Scalate Precompilation and Bindings
scalateTemplateConfig in Compile <<= (resourceDirectory in Compile) { base => 
  println ("bbb:" + base)
  Seq(
    TemplateConfig(
      base / "shark" / "execution" / "cg" / "operator",
      Nil,
      Nil,
      Some("shark.execution.cg.operator")
    ),  
    TemplateConfig(
      base / "shark" / "execution" / "cg" / "row",
      Nil,
      Nil,
      Some("shark.execution.cg.row")
    )/*,  
    TemplateConfig(
      base / "shark" / "execution" / "cg" / "row" / "oi",
      Nil,
      Nil,
      Some("shark.execution.cg.row.oi")
    ) */  
  )
}
