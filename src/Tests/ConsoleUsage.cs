namespace Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using ICSharpCode.Decompiler;
    using ICSharpCode.Decompiler.CSharp;
    using ICSharpCode.Decompiler.CSharp.Syntax;
    using NUnit.Framework;

    public class ConsoleUsage
    {
        [Test]
        public void DontUseConsoleDirectly()
        {
            var assembly = typeof(Out).Assembly;

            var decompiler = new CSharpDecompiler(assembly.Location, new DecompilerSettings());

            var types = assembly.GetTypes()
                .Where(t => t != typeof(Out))
                .Where(t => t.DeclaringType is null)
                .Where(t => t.GetCustomAttribute<CompilerGeneratedAttribute>() == null)
                .ToArray();

            Console.WriteLine($"{types.Length} types found.");

            var foundIn = new List<string>();

            foreach (var type in types)
            {
                Console.WriteLine(type);
                var decompiledType = decompiler.DecompileType(new ICSharpCode.Decompiler.TypeSystem.FullTypeName(type.FullName));

                var consoleInvocations = decompiledType.DescendantNodesAndSelf()
                    .OfType<InvocationExpression>()
                    .Select(invoke => new { Invocation = invoke, Name = invoke.FirstChild.ToString() })
                    .Where(invoke => invoke.Name.StartsWith("Console.") || invoke.Name.StartsWith("System.Console."))
                    .Select(invoke =>
                    {
                        var ancestors = invoke.Invocation.Ancestors.OfType<EntityDeclaration>().Reverse().Select(e => e.Name).ToArray();
                        return "Found reference to System.Console in " + string.Join(" : ", ancestors);
                    });

                foundIn.AddRange(consoleInvocations);
            }

            Assert.That(foundIn, Is.Empty, "Detected usage of System.Console methods. Use static Out class instead which is a wrapper so that console output can be sent for error analysis.");
        }
    }
}
